from pyspark.sql import SparkSession
from datetime import datetime
import logging
import cx_Oracle

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Reconciliation") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()

# Initialize Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Connection Setup
oracle_url = "jdbc:oracle:thin:@your_oracle_db_url"
oracle_user = "your_oracle_user"
oracle_password = "your_oracle_password"
mongo_uri = "your_mongo_uri"

# Connect to Oracle DB
conn = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_url)
cursor = conn.cursor()

# Step 1: Fetch Pending Batches
batch_df = spark.read \
    .format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "SparkReconBatchTable") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .load()

pending_batches = batch_df.filter(batch_df.RECONCILE_DTTM.isNull()).collect()

# Step 2: Load Data from Source and Target
for batch in pending_batches:
    batch_start_date = datetime.strftime(batch['BATCH_START_DATE'], "%d-%b-%Y %H:%M:%S.%f")
    batch_end_date = datetime.strftime(batch['BATCH_END_DATE'], "%d-%b-%Y %H:%M:%S.%f")

    sql_query = f"""
        SELECT * 
        FROM source_table d
        WHERE d.uab78_division = 'AML' 
        AND d.modify_date > TO_TIMESTAMP('{batch_start_date}', 'DD-MON-YYYY HH24:MI:SS.FF6')
        AND d.modify_date <= TO_TIMESTAMP('{batch_end_date}', 'DD-MON-YYYY HH24:MI:SS.FF6')
    """

    source_df = spark.read \
        .format("jdbc") \
        .option("url", oracle_url) \
        .option("dbtable", f"({sql_query}) src") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .load()

    target_df = spark.read \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("collection", "target_collection") \
        .load() \
        .filter(f"batchID = '{batch['BATCHID']}'")

    # Step 3.1: Compare Source and Target Counts
    source_count = source_df.count()
    target_count = target_df.count()

    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_SRC_CNT = {source_count}, 
            RECONCILE_TGT_CNT = {target_count} 
        WHERE BATCHID = '{batch['BATCHID']}'
    """
    cursor.execute(update_query)
    conn.commit()

    # Step 3.2: Data Comparison
    excluded_columns = ["transformation_column1", "transformation_column2"]
    comparison_df = source_df.select([col for col in source_df.columns if col not in excluded_columns]) \
        .subtract(target_df.select([col for col in target_df.columns if col not in excluded_columns]))

    reconcile_check = "MATCHED" if comparison_df.count() == 0 else "MISMATCHED"

    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_CHECK = '{reconcile_check}' 
        WHERE BATCHID = '{batch['BATCHID']}'
    """
    cursor.execute(update_query)
    conn.commit()

    # Step 3.3: Retrieve and Validate GUIDs
    guid_df = target_df.select("GUID", "StoragePath").limit(100)

    def validate_storage_path(row):
        if hitachi_api.check_path(row['StoragePath']):  # Assuming hitachi_api.check_path is a predefined function
            return (row['GUID'], "SUCCESS")
        else:
            return (row['GUID'], "FAILURE")

    validation_results = guid_df.rdd.map(validate_storage_path).collect()

    for guid, status in validation_results:
        update_query = f"""
            UPDATE SparkReconBatchTable 
            SET RETRIEVAL_VALIDATION = '{status}' 
            WHERE GUID = '{guid}'
        """
        cursor.execute(update_query)
        conn.commit()

    # Step 4: Mark Reconciliation Completion
    current_time = datetime.now().strftime("%d-%b-%Y %H:%M:%S.%f")
    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_DTTM = TO_TIMESTAMP('{current_time}', 'DD-MON-YYYY HH24:MI:SS.FF6') 
        WHERE BATCHID = '{batch['BATCHID']}'
    """
    cursor.execute(update_query)
    conn.commit()

    # Logging Reconciliation Time
    logger.info(f"Reconciliation completed for BATCHID={batch['BATCHID']} in {reconciliation_time} seconds.")

# Close Oracle connection
cursor.close()
conn.close()

# Stop Spark session
spark.stop()
