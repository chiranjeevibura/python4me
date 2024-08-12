import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import configparser
import pymongo
import sqlalchemy
import requests

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

# HCP Config
hcp_server_url = config['hcp']['hcp_server_url']
hcp_authentication_code = config['hcp']['hcp_authentication_code']

# Queries
batch_table_query = config['queries']['batch_table_query']
main_sql_query = config['queries']['main_sql_query']
cust_sql_query = config['queries']['cust_sql_query']
ext_sql_query = config['queries']['ext_sql_query']
update_batch_status_query = config['queries']['update_batch_status_query']

# Initialize Spark session
spark = SparkSession.builder \
  .appName("Oracle to MongoDB Reconciliation") \
  .config("spark.mongodb.read.connection.uri", mongo_uri) \
  .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.2') \
  .getOrCreate()

# Initialize SQLAlchemy engine for Oracle database updates
engine = sqlalchemy.create_engine(f"oracle://{oracle_user}:{oracle_password}@{oracle_url}")

def reconcile_batch(batch_id, batch_start_date, batch_end_date):
  try:
    # Query source Oracle database for document count
    source_count_query = f"""
      SELECT COUNT(*) AS source_count
      FROM your_source_table
      WHERE created_date BETWEEN TO_DATE('{batch_start_date}', 'DD-MON-YY HH24.MI.SS.FF')
                             AND TO_DATE('{batch_end_date}', 'DD-MON-YY HH24.MI.SS.FF')
    """
    source_count = pd.read_sql(source_count_query, engine)['source_count'][0]

    # Query target MongoDB database for document count
    target_count_query = f"""
      db.{collection}.find({{'createdDate': {{'$gte': '{batch_start_date}', '$lt': '{batch_end_date}'}}}}).count()
    """
    target_count = spark.read.format("mongodb").option("uri", mongo_uri).option("database", database).option("collection", collection).option("query", target_count_query).load().collect()[0][0]

    # Compare key columns
    source_df = spark.read.format("jdbc") \
      .option("url", oracle_url) \
      .option("dbtable", "your_source_table")  # Replace with your source table name
      .option("user", oracle_user) \
      .option("password", oracle_password) \
      .option("driver", oracle_driver) \
      .load() \
      .filter(F.col("createdDate").between(F.lit(batch_start_date), F.lit(batch_end_date))) \
      .select("DOMAIN", "DIVISION", "SUBDIVISION", "CUSTREFKEY", "CUSTREFValue", "EXTREFKEY", "EXTREFValue")

    target_df = spark.read.format("mongodb").option("uri", mongo_uri).option("database", database).option("collection", collection).load() \
      .filter(F.col("createdDate").between(F.lit(batch_start_date), F.lit(batch_end_date))) \
      .select("DOMAIN", "DIVISION", "SUBDIVISION", "CUSTREFKEY", "CUSTREFValue", "EXTREFKEY", "EXTREFValue")

    # Join DataFrames for comparison
    comparison_df = source_df.join(target_df, ["DOMAIN", "DIVISION", "SUBDIVISION", "CUSTREFKEY", "CUSTREFValue", "EXTREFKEY", "EXTREFValue"], how="full_outer")
    mismatch_count = comparison_df.filter((comparison_df["DOMAIN"].isNull()) | (comparison_df["DIVISION"].isNull()) | (comparison_df["SUBDIVISION"].isNull()) | (comparison_df["CUSTREFKEY"].isNull()) | (comparison_df["CUSTREFValue"].isNull()) | (comparison_df["EXTREFKEY"].isNull()) | (comparison_df["EXTREFValue"].isNull())).count()

    # Retrieve random 10 document GUIDs and HCP storage paths from MongoDB
    guid_df = spark.read.format("mongodb").option("uri", mongo_uri).option("database", database).option("collection", collection).load() \
      .filter(F.col("batchId") == batch_id) \
      .select("guid", "HCPstoragePath") \
      .sample(False, 0.1)  # Adjust sampling fraction as needed

    guid_list = guid_df.rdd.flatMap(lambda x: (x['guid'], x['HCPstoragePath'])).collect()

    # Validate documents and build validation results
    validation_results = []
    for guid, hcp_storage_path in guid_list:
      status = validate_document(hcp_storage_path)
      validation_results.append(f"{guid}|{status}")

    # Update SparkReconBatchTable with results
    update_recon_batch_table(batch_id, source_count, target_count, mismatch_count, validation_results)

  except Exception as e:
    logger.error(f"Error during reconciliation for batch {batch_id}: {e}")

def validate_document(hcp_storage_path):
  # Replace with your actual HCP validation logic
  try:
    # Construct HCP API request
    url = f"{hcp_server_url}/rest/objects/{hcp_storage_path}"
    headers = {'Authorization': hcp_authentication_code}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
      return "Success"
    else:
      return f"Failure: {response.status_code} {response.reason}"
  except Exception as e:
    return f"Failure: {str(e)}"

def update_recon_batch_table(batch_id, source_count, target_count, mismatch_count, validation_results):
  # Update SparkReconBatchTable with reconciliation results
  update_query = f"""
  UPDATE SparkReconBatchTable
  SET RECONCILE_SRC_CNT = {source_count},
      RECONCILE_TGT_CNT = {target_count},
      MISMATCH_CNT = {mismatch_count},
      RETRIEVAL_VALIDATION = '{','.join(validation_results)}'
  WHERE BATCHID = '{batch_id}'
  """
  with engine.connect() as conn:
    conn.execute(update_query)

def main():
  while True:
    try:
      batch_df = spark.read.format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", batch_table_query) \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", oracle_driver) \
        .load()

      for batch in batch_df.collect():
        batch_id = batch['BATCHID']
        batch_start_date = batch['BATCH_START_DATE']
        batch_end_date = batch['BATCH_END_DATE']
        reconcile_batch(batch_id, batch_start_date, batch_end_date)

      time.sleep(60)  # Adjust sleep time as needed

    except Exception as e:
      logger.error(f"Unexpected error: {e}")

if __name__ == "__main__":
  main()
