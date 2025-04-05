import sys
import os
import datetime
import logging
from pyspark.sql import SparkSession
import boto3
import json
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType

# Set up logging configuration
logging.basicConfig(level=logging.INFO)  # Adjust level to DEBUG, INFO, WARNING as needed
logger = logging.getLogger(__name__)

# Retrieve arguments passed to the script
args = sys.argv
logger.info(f"Arguments received: {args}")
secret_name = args[args.index('--secret_name') + 1]  # Secret name
aws_region = args[args.index('--aws_region') + 1]  # AWS region
destination_bucket = args[args.index('--destination_bucket') + 1]  # Destination bucket
source_table = args[args.index('--source_table') + 1]  # Source table

logger.info(f"Argument1 Secret name         : {secret_name}")
logger.info(f"Argument2 AWS region          : {aws_region}")
logger.info(f"Argument3 Destination bucket  : {destination_bucket}")
logger.info(f"Argument5 Source table        : {source_table}")

# Initialize Spark session with Hudi dependencies
spark = SparkSession.builder \
    .appName("Practicecalls_datalakehouse") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.20,"
                                  "org.apache.hudi:hudi-spark3-bundle_2.12:0.14.0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .getOrCreate()

# First, clean up the existing Hudi metadata to resolve versioning issues
try:
    # Delete the metadata directory if it exists
    s3_client = boto3.client('s3', region_name=aws_region)
    bucket_name = destination_bucket.replace('s3://', '').split('/')[0]
    prefix_parts = destination_bucket.replace('s3://', '').split('/')
    prefix = '/'.join(prefix_parts[1:]) if len(prefix_parts) > 1 else ""
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    prefix += '.hoodie/'
    
    logger.info(f"Attempting to clear Hudi metadata at bucket: {bucket_name}, prefix: {prefix}")
    
    # List objects in the .hoodie directory
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        logger.info(f"Deleted Hudi metadata in {prefix}")
    else:
        logger.info(f"No Hudi metadata found at {prefix}")
except Exception as e:
    logger.error(f"Error clearing Hudi metadata: {e}")

# Fetch secrets from AWS Secrets Manager
def get_secret(secret_name, region_name=aws_region):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Get PostgreSQL credentials from Secrets Manager
secrets = get_secret(secret_name)

# PostgreSQL connection properties
jdbc_url = f"jdbc:postgresql://{secrets['POSTGRES_HOST']}:{secrets['POSTGRES_PORT']}/{secrets['POSTGRES_DB']}"
properties = {
    "user": secrets["POSTGRES_USER"],
    "password": secrets["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL
df = spark.read.jdbc(url=jdbc_url, table=source_table, properties=properties)

# Log the DataFrame schema
logger.info(f"Original DataFrame schema: {df.schema}")
logger.info(f"Original DataFrame columns: {df.columns}")

# Add the specific partition column that's expected
df = df.withColumn("partition_column", col("end_date_time").cast(StringType()))

# Define Hudi write options with specific focus on schema handling
hudi_options = {
    "hoodie.table.name": "mockrecord_hudi",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "end_date_time",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    
    # Disable schema validation and configure for schema evolution
    "hoodie.avro.schema.validate": "false",
    "hoodie.datasource.write.reconcile.schema": "true",
    "hoodie.schema.on.read.enable": "true",
    
    # Configure partitioning
    "hoodie.datasource.write.partitionpath.field": "partition_column",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
    
    # Hive sync options
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "default",
    "hoodie.datasource.hive_sync.table": "mockrecord_hudi",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.partition_fields": "partition_column",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    
    # Additional performance settings
    "hoodie.parquet.small.file.limit": "104857600",
    "hoodie.clustering.inline": "true",
    "hoodie.clean.automatic": "true"
}

# Log the Hudi options being used
logger.info(f"Hudi options: {hudi_options}")

# Check if we need to clear any existing table in the Glue catalog
try:
    glue_client = boto3.client('glue', region_name=aws_region)
    try:
        # Check if table exists and log its schema
        response = glue_client.get_table(
            DatabaseName='default',
            Name='mockrecord_hudi'
        )
        logger.info(f"Existing table in Glue catalog. Attempting to delete.")
        glue_client.delete_table(DatabaseName='default', Name='mockrecord_hudi')
        logger.info("Successfully deleted existing table from Glue catalog.")
    except Exception as e:
        logger.info(f"Table not found in Glue catalog or error checking: {e}")
except Exception as e:
    logger.error(f"Error with Glue operations: {e}")

# Write DataFrame to S3 in Hudi Format
try:
    logger.info(f"Writing data to destination: {destination_bucket}")
    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(destination_bucket)
    logger.info("Successfully wrote data to Hudi format")
except Exception as e:
    logger.error(f"Error writing to Hudi: {e}")
    
    # If still failing, try with a completely new table name
    try:
        logger.info("Attempting with new table name as fallback")
        new_options = hudi_options.copy()
        new_options["hoodie.table.name"] = "mockrecord_hudi_new"
        new_options["hoodie.datasource.hive_sync.table"] = "mockrecord_hudi_new"
        
        df.write.format("org.apache.hudi") \
            .options(**new_options) \
            .mode("append") \
            .save(destination_bucket + "_new")
        logger.info("Successfully wrote data with new table name")
    except Exception as e2:
        logger.error(f"Fallback approach also failed: {e2}")

# Stop Spark session
spark.stop()