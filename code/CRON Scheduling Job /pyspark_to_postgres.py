import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lower, col, to_date, datediff, coalesce, unix_timestamp, when, lit, hash, abs, concat
from datetime import datetime
from datetime import timedelta
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import when


# Replace these with the API URL and PostgreSQL connection details
api_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
db_url = "jdbc:postgresql://localhost:5432/311_service_data"
postgres_properties = {
    "user": "postgres",
    "password": "123",
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified"
}


# Get today's date in the format needed for the API
today = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
api_url = f"{api_url}?$where=created_date >= '{today}'"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("API to PySpark to PostgreSQL") \
    .getOrCreate()

# Fetch data from API
response = requests.get(api_url)
data = response.json()

# Create a DataFrame from the JSON data
df_raw = spark.read.json(spark.sparkContext.parallelize(data))

# Drop existing 'location' column
df_raw = df_raw.drop("location")

# Add a new column with today's date and time
df_raw = df_raw.withColumn('LoadTimestamp', current_timestamp())

# Convert string columns to lowercase
string_columns = ["agency", "complaint_type","city", "borough", "incident_address", "status"]
for column in string_columns:
    df_raw = df_raw.withColumn(column, lower(col(column)))

# Format Created Date
date_format = "yyyy-MM-dd'T'HH:mm:ss.SSS"
df_raw = df_raw.withColumn("created_date", to_date(unix_timestamp(col("created_date"), date_format).cast("timestamp")))

# Format Closed Date
df_raw = df_raw.withColumn("closed_date", when(col("closed_date").isNotNull(), to_date(unix_timestamp(col("closed_date"), date_format).cast("timestamp"))))

# Calculate the difference between Closed Date and Created Date
df_raw = df_raw.withColumn("DaysToClose", when(col("closed_date").isNull(), 0).when(col("closed_date") == col("created_date"), 1).otherwise(datediff(col("closed_date"), col("created_date"))))

# Add 'location' column with format (latitude,longitude)
df_raw = df_raw.withColumn("location", concat(lit("("), col("latitude"), lit(","), col("longitude"), lit(")")))

# Select columns to check for uniqueness
loc = ['location']

# Create a new DataFrame with only unique rows based on the selected columns
loc_df = df_raw.drop_duplicates(loc)

# Select specific columns
loc_df = loc_df.withColumn('LocID', abs(hash("location")))
loc_df = loc_df.withColumn("Latitude", col("Latitude").cast(DoubleType()))
loc_df = loc_df.withColumn("Longitude", col("Longitude").cast(DoubleType()))

loc_df = loc_df.select("LocID", "location", "latitude", "longitude", "incident_zip", "incident_address", "borough", "city")

# Register DataFrames as temporary views
df_raw.createOrReplaceTempView("df_raw")
loc_df.createOrReplaceTempView("loc_df")

# SQL query to join DataFrames
joined_df = spark.sql("""
    SELECT unique_key, agency, complaint_type, status, closed_date, created_date, LoadTimestamp, DaystoClose, a.LocID as location_id
    FROM df_raw d
    JOIN loc_df a ON d.Location = a.Location
""")


# Rename columns in joined_df
joined_df = joined_df.withColumnRenamed("unique_key", "Unique Key") \
                     .withColumnRenamed("agency", "Agency") \
                     .withColumnRenamed("complaint_type", "Complaint Type") \
                     .withColumnRenamed("status", "Status") \
                     .withColumnRenamed("closed_date", "Closed Date") \
                     .withColumnRenamed("created_date", "Created Date")

# Rename columns in loc_df
loc_df = loc_df.withColumnRenamed("loc_id", "LocID") \
               .withColumnRenamed("incident_zip", "Incident Zip") \
               .withColumnRenamed("incident_address", "Incident Address") \
               .withColumnRenamed("location", "Location") \
               .withColumnRenamed("latitude", "Latitude") \
               .withColumnRenamed("longitude", "Longitude") \
               .withColumnRenamed("borough", "Borough") \
               .withColumnRenamed("city", "City")

# Show DataFrames
joined_df.show()
loc_df.show()


existing_loc_df = spark.read.jdbc(db_url, "location", properties=postgres_properties)
existing_loc_df.createOrReplaceTempView("existing_loc_df")
new_loc_df = loc_df.join(existing_loc_df, loc_df["LocID"] == existing_loc_df["LocID"], "left_anti")



new_loc_df.write.jdbc(db_url, "location", mode="append", properties=postgres_properties)

# Write joined_df to PostgreSQL
joined_df.write.jdbc(db_url, "main_request", mode="append", properties=postgres_properties)

spark.stop()
