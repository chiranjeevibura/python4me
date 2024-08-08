import io
import uuid
import logging
import time
import pytz
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType
import configparser
import pymongo

# Configuration
config = configparser.ConfigParser()
config.read('config.ini')

# MongoDB Config
mongo_uri = config['mongo']['uri']
database = config['mongo']['database']
collection = config['mongo']['collection']

# OracleDB Config
oracle_user = config['oracle']['user']
oracle_password = config['oracle']['password']
oracle_driver = config['oracle']['driver']
oracle_url = config['oracle']['url']

# Queries
main_sql_query = config['queries']['main_sql_query']
cust_sql_query = config['queries']['cust_sql_query']
ext_sql_query = config['queries']['ext_sql_query']

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Oracle to MongoDB Migration") \
    .config("spark.mongodb.read.connection.uri", mongo_uri) \
    .config("spark.mongodb.write.connection.uri", mongo_uri) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.2') \
    .config('spark.mongodb.write.convertJson', "object_Or_Array_Only") \
    .getOrCreate()

# Read data from Oracle database
def read_data_from_oracle():
    try:
        oracle_main_df = spark.read.format("jdbc") \
            .option("url", oracle_url) \
            .option("dbtable", f"({sql_main_query})") \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", oracle_driver) \
            .load()

        oracle_ext_df = spark.read.format("jdbc") \
            .option("url", oracle_url) \
            .option("dbtable", f"({sql_ext_ref_query})") \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", oracle_driver) \
            .load()

        oracle_cust_df = spark.read.format("jdbc") \
            .option("url", oracle_url) \
            .option("dbtable", f"({sql_cust_ref_query})") \
            .option("user", oracle_user) \
            .option("password", oracle_password) \
            .option("driver", oracle_driver) \
            .load()

        # Aggregate externalReferenceKeys in oracle_ext_df
        oracle_ext_df = oracle_ext_df.groupBy("object_id").agg(
            F.collect_list(
                F.struct(
                    F.col("ext_ref_domain").alias("domain"),
                    F.col("externalReferenceKeys_name").alias("name"),
                    F.col("externalReferenceKeys_value").alias("value")
                )
            ).alias("externalReferenceKeys")
        )

        # Aggregate customerReferenceKeys in oracle_cust_df
        oracle_cust_df = oracle_cust_df.groupBy("object_id").agg(
            F.collect_list(
                F.struct(
                    F.col("CustomerDomain").alias("domain"),
                    F.col("customerReferenceKeys_name").alias("name"),
                    F.col("customerReferenceKeys_value").alias("value")
                )
            ).alias("customerReferenceKeys")
        )

        # Join main_df with oracle_ext_df and oracle_cust_df
        oracle_df = oracle_main_df.join(oracle_ext_df, on="object_id", how="left") \
                                 .join(oracle_cust_df, on="object_id", how="left")

        return oracle_df
    except Exception as e:
        logger.error(f"Error reading data from Oracle: {e}")
        raise

# Function to decode Oracle Blob
def get_hcp_path(hop_blob_data):
    # ... (existing code)

# Function to replace values
def replace_with_value(column_source_value, column_name):
    # ... (existing code)

# Function to generate UUID
def generate_uuid_id(object_id):
    # ... (existing code)

# Function to generate batchID
def generate_batch_id():
    return str(uuid.uuid4().hex)[:8]
batch_id = generate_batch_id()

# Function to split document Title into BaseName and Extension
def split_document_title(documentTitle):
    # ... (existing code)

# Define Transformations 
def transform(oracle_df):
    try:
        start_time = time.time()

        # ... (existing transformation logic)

        return transformed_df
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        # Handle errors (e.g., skip problematic rows, log, retry)

# Function to write to Mongo DB
def load_to_mongo(transformed_df):
    try:
        start_time = time.time()
        transformed_df.write.format("mongodb") \
            .mode("append") \
            .option("database", database) \
            .option("collection", collection) \
            .save()

        mongo_num_rows = transformed_df.count()
        logger.info(f"Number of rows written to Mongo Collection: {mongo_num_rows}")
        logger.info(f"Data write completed in {time.time() - start_time} seconds")
    except pymongo.errors.BulkWriteError as e:
        # Handle specific MongoDB bulk write errors
        logger.error(f"Bulk write error: {e}")
    except Exception as e:
        logger.error(f"Error writing to MongoDB: {e}")

# Function to calculate migration time
def calculate_migration_time(start_time)
    end_time = datetime.now(pytz.utc)
    migration_time = end_time - start_time
    print(f"migration time: {migration_time}")

if __name__ == "__main__":
    start_time = datetime.now(pytz.utc)
    logger.info("Migration started")

    try:
        # Read data from Oracle
        start_time_read = datetime.now()
        oracle_df = read_data_from_oracle()

        if oracle_df.count() > 0:
            end_time_read = datetime.now()
            time_taken_read = end_time_read - start_time_read

            # Perform transformations
            start_time_transform = datetime.now()
            transform_df = transform(oracle_df)
            end_time_transform = datetime.now()
            time_taken_transform = end_time_transform - start_time_transform

            # Write data to MongoDB
            start_time_write = datetime.now()
            load_to_mongo(transform_df)

            # Calculate migration time
            calculate_migration_time(start_time)
        else:
            print("No new records found in Source")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    logger.info("Migration completed")
    spark.stop()
