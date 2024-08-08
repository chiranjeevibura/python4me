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
import sqlalchemy

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
batch_table_query = config['queries']['batch_table_query']
main_sql_query = config['queries']['main_sql_query']
cust_sql_query = config['queries']['cust_sql_query']
ext_sql_query = config['queries']['ext_sql_query']
update_batch_status_query = config['queries']['update_batch_status_query']

# Initialize Spark session
spark = SparkSession.builder \
  .appName("Oracle to MongoDB Migration") \
  .config("spark.mongodb.read.connection.uri", mongo_uri) \
  .config("spark.mongodb.write.connection.uri", mongo_uri) \
  .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.2') \
  .config('spark.mongodb.write.convertJson', "object_Or_Array_Only") \
  .getOrCreate()

# Initialize SQLAlchemy engine for Oracle database updates
engine = sqlalchemy.create_engine(f"oracle://{oracle_user}:{oracle_password}@{oracle_url}")

# Read data from Oracle database
def read_data_from_oracle():
  try:
    # Read batch information
    batch_df = spark.read.format("jdbc") \
      .option("url", oracle_url) \
      .option("dbtable", batch_table_query) \
      .option("user", oracle_user) \
      .option("password", oracle_password) \
      .option("driver", oracle_driver) \
      .load()

    # Validate if there's a new batch to process
    if not batch_df.isEmpty():
      current_batch_id = batch_df.select("batchID").first()[0]
      from_date = batch_df.select("FromDate").first()[0]
      to_date = batch_df.select("ToDate").first()[0]

      # Construct dynamic main query with date filter
      dynamic_main_sql_query = f"""{main_sql_query} WHERE CreatedDate >= TO_DATE('{from_date}', 'YYYY-MM-DD') AND CreatedDate < TO_DATE('{to_date}', 'YYYY-MM-DD') + interval '1 day'"""

      # Read main data based on batch information
      oracle_main_df = spark.read.format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", f"({dynamic_main_sql_query})") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", oracle_driver) \
        .load()

      # ... (rest of the code for reading reference data and joining)

      return oracle_df, current_batch_id
    else:
      logger.info("No new batch found for processing")
      return None, None
  except Exception as e:
    logger.error(f"Error reading data from Oracle: {e}")
    raise

# ... (rest of the functions)

def update_batch_status(batch_id):
  try:
    with engine.connect() as conn:
      conn.execute(update_batch_status_query, {'batch_id': batch_id, 'migration_status': datetime.now()})
  except Exception as e:
    logger.error(f"Error updating batch status: {e}")

if __name__ == "__main__":
  start_time = datetime.now(pytz.utc)
  logger.info("Migration started")

  try:
    while True:
      oracle_df, batch_id = read_data_from_oracle()
      if oracle_df is not None:
        transform_df = transform(oracle_df)
        load_to_mongo(transform_df, batch_id)
        update_batch_status(batch_id)
        calculate_migration_time(start_time)
      else:
        logger.info("No new batches to process. Sleeping for 5 minutes")
        time.sleep(300)  # Sleep for 5 minutes

  except Exception as e:
    logger.error(f"Unexpected error: {e}")

  logger.info("Migration completed")
  spark.stop()
