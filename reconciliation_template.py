import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import configparser
import pymongo
import sqlalchemy
import random

# ... (other imports and configurations)

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

    # Compare key columns (implement logic to compare DataFrames)
    # ...

    # Document validation (implement logic to randomly sample documents and validate on HCP)
    # ...

    # Update SparkReconBatchTable with results
    # ...

  except Exception as e:
    logger.error(f"Error during reconciliation for batch {batch_id}: {e}")

def main():
  # Read batch information from Oracle
  batch_df = spark.read.format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "SparkReconBatchTable")  # Replace with your table name
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .load()

  # Iterate over batches
  for batch in batch_df.collect():
    batch_id = batch['BATCHID']
    batch_start_date = batch['BATCH_START_DATE']
    batch_end_date = batch['BATCH_END_DATE']
    reconcile_batch(batch_id, batch_start_date, batch_end_date)

if __name__ == "__main__":
  main()

###############
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import configparser
import pymongo
import sqlalchemy
import random

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

        # Document validation (placeholder, implement logic)
        validation_results = []  # Replace with actual validation logic

        # Update SparkReconBatchTable with results
        update_recon_batch_table(batch_id, source_count, target_count, mismatch_count, validation_results)

  except Exception as e:
    logger.error(f"Error during reconciliation for batch {batch_id}: {e}")

# ... (rest of the code)

def update_recon_batch_table(batch_id, source_count, target_count, mismatch_count, validation_results):
  # Update SparkReconBatchTable with reconciliation results
  update_query = f"""
  UPDATE SparkReconBatchTable
  SET RECONCILE_SRC_CNT = {source_count},
      RECONCILE_TGT_CNT = {target_count},
      MISMATCH_CNT = {mismatch_count},
      VALIDATION_RESULTS = '{validation_results}'
  WHERE BATCHID = '{batch_id}'
  """
  with engine.connect() as conn:
    conn.execute(update_query)


###################

