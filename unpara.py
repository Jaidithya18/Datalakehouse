import sys
import boto3
import json
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, weekofyear, to_date, max as spark_max
from datetime import datetime

# Set the AWS region
aws_region = "us-east-2"  # Replace with your AWS region
os.environ['AWS_REGION'] = aws_region

# Function to get secrets from AWS Secrets Manager
def get_secret(secret_name):
    try:
        client = boto3.client('secretsmanager', region_name=aws_region)
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        print("Secrets retrieved successfully")
        return secret
    except Exception as e:
        print(f"Error retrieving secrets: {e}")
        raise

# Get PostgreSQL connection details from AWS Secrets Manager
try:
    secret_name = "Secrets"
    secrets = get_secret(secret_name)
    postgres_host = secrets['POSTGRES_HOST']
    postgres_user = secrets['POSTGRES_USER']
    postgres_password = secrets['POSTGRES_PASSWORD']
    postgres_db = secrets['POSTGRES_DB']
    postgres_port = secrets['POSTGRES_PORT']
    print("PostgreSQL connection details retrieved")
except Exception as e:
    print(f"Error retrieving PostgreSQL connection details: {e}")
    raise

# Initialize Spark session
try:
    spark = SparkSession.builder \
        .appName("IncrementalLoadJob") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3-bundle_2.12:0.13.1") \
        .getOrCreate()
    print("Spark session initialized")
except Exception as e:
    print(f"Error initializing Spark session: {e}")
    raise

# Define paths or tables for the source and destination data
source_table = "public.mockrecord"  # Replace with your actual source table or path
destination_bucket = "s3a://datalakehouse-j/datalake"  # Replace with your actual S3 bucket and path
hudi_table_path = f"{destination_bucket}/hudi_table/"

# PostgreSQL connection properties
jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

# Function to check if the Hudi table exists
def hudi_table_exists(hudi_table_path):
    try:
        spark.read.format("hudi").load(hudi_table_path).limit(1).collect()
        return True
    except:
        return False

# Function to get the last checkpoint time from the Hudi table
def get_last_checkpoint_time(hudi_table_path):
    try:
        commits = spark.read.format("hudi").load(hudi_table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time", ascending=False).limit(1).collect()
        if commits:
            last_commit = commits[0]["_hoodie_commit_time"]
            # Convert last_commit to standard datetime format
            last_commit_time = datetime.strptime(last_commit, '%Y%m%d%H%M%S%f').strftime('%Y-%m-%d %H:%M:%S')
            return last_commit_time
        else:
            return '1970-01-01 00:00:00'  # Use the earliest possible timestamp in your dataset
    except Exception as e:
        print(f"Error retrieving last checkpoint time: {e}")
        raise

# Check if the Hudi table exists
table_exists = hudi_table_exists(hudi_table_path)
print(f"Hudi table exists: {table_exists}")

# Read new or updated data from the source based on the last checkpoint time if the table exists
if table_exists:
    try:
        last_checkpoint_time = get_last_checkpoint_time(hudi_table_path)
        print(f"Last checkpoint time: {last_checkpoint_time}")
    except Exception as e:
        print(f"Error getting last checkpoint time: {e}")
        raise

    try:
        query = f"(SELECT * FROM {source_table} WHERE end_date_time > '{last_checkpoint_time}') AS new_data"
        print("Executing query:", query)
        new_data = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .load()
        print("New or updated data loaded from PostgreSQL")
    except Exception as e:
        print(f"Error loading data from PostgreSQL: {e}")
        raise
else:
    # For the first run, load all data
    try:
        query = f"(SELECT * FROM {source_table}) AS new_data"
        print("Executing query:", query)
        new_data = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .option("driver", properties["driver"]) \
            .load()
        print("All data loaded from PostgreSQL")
    except Exception as e:
        print(f"Error loading data from PostgreSQL: {e}")
        raise

# Add year, month, and week columns to the DataFrame
new_data = new_data.withColumn("year", year(to_date(col("end_date_time")))) \
                   .withColumn("month", month(to_date(col("end_date_time")))) \
                   .withColumn("week", weekofyear(to_date(col("end_date_time"))))

# Ensure destination_bucket path is correct
assert destination_bucket.startswith("s3a://"), "Invalid S3 bucket path format"

# Write the new or updated data to the destination (S3 bucket) with Hudi
try:
    hudi_options = {
        'hoodie.table.name': 'hudi_table',
        'hoodie.datasource.write.storage.type': 'COPY_ON_WRITE',  # Use Copy-on-Write storage type
        'hoodie.datasource.write.recordkey.field': 'id',  # Replace 'id' with your record key field
        'hoodie.datasource.write.partitionpath.field': 'year,month,week',
        'hoodie.datasource.write.table.name': 'hudi_table',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'end_date_time',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2,
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.mode': 'hms',  # Use 'hms' for AWS Glue Metastore
        'hoodie.datasource.hive_sync.use_glue_catalog': 'true',  # Enable Glue
        'hoodie.datasource.hive_sync.database': 'default',
        'hoodie.datasource.hive_sync.table': 'hudi_table',
        'hoodie.datasource.hive_sync.partition_fields': 'year,month,week',
    }

    # Validate Hudi options
    required_hudi_options = [
        'hoodie.table.name',
        'hoodie.datasource.write.storage.type',
        'hoodie.datasource.write.recordkey.field',
        'hoodie.datasource.write.partitionpath.field',
        'hoodie.datasource.write.table.name',
        'hoodie.datasource.write.operation',
        'hoodie.datasource.write.precombine.field',
        'hoodie.upsert.shuffle.parallelism',
        'hoodie.insert.shuffle.parallelism',
        'hoodie.datasource.hive_sync.enable',
        'hoodie.datasource.hive_sync.mode',
        'hoodie.datasource.hive_sync.use_glue_catalog',
        'hoodie.datasource.hive_sync.database',
        'hoodie.datasource.hive_sync.table',
        'hoodie.datasource.hive_sync.partition_fields',
    ]

    for option in required_hudi_options:
        assert option in hudi_options, f"Missing Hudi option: {option}"

    print("Hudi options:", hudi_options)
    print("Destination bucket:", destination_bucket + "/hudi_table/")

    # Validate new_data DataFrame
    if new_data.rdd.isEmpty():
        print("No new data to write.")
    else:
        print("new_data schema:")
        new_data.printSchema()

        new_data.write.format("org.apache.hudi"). \
            options(**hudi_options). \
            mode("append"). \
            save(destination_bucket + "/hudi_table/")
        
        print(f"New data written to {destination_bucket} with Hudi")
except Exception as e:
    print(f"Error writing data to S3 with Hudi: {e}")
    raise

# Stop the Spark session
try:
    spark.stop()
    print("Spark session stopped")
except Exception as e:
    print(f"Error stopping Spark session: {e}")
    raise