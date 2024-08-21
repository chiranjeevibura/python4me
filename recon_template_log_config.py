from pyspark.sql import SparkSession
from datetime import datetime
import logging
import configparser
import os
import shutil
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import cx_Oracle
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Reconciliation") \
    .config("spark.some.config.option", "config-value") \
    .getOrCreate()

# Load Configuration
config = configparser.ConfigParser()
config.read('config.ini')

# Database and HCP Configuration
oracle_url = config.get('oracle', 'url')
oracle_user = config.get('oracle', 'user')
oracle_password = config.get('oracle', 'password')
oracle_driver = config.get('oracle', 'driver')

mongo_uri = config.get('mongodb', 'uri')

hcp_url = config.get('hcp', 'url')
auth_code = config.get('hcp', 'auth_code')

log_directory = config.get('paths', 'log_directory')
save_directory = config.get('paths', 'save_directory')

# Database Connection Setup
conn = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=oracle_url)
cursor = conn.cursor()

# Step 1: Fetch Pending Batches
batch_df = spark.read \
    .format("jdbc") \
    .option("url", oracle_url) \
    .option("dbtable", "SparkReconBatchTable") \
    .option("user", oracle_user) \
    .option("password", oracle_password) \
    .option("driver", oracle_driver) \
    .load()

pending_batches = batch_df.filter(batch_df.RECONCILE_DTTM.isNull()).collect()

# Step 2: Process Each Batch
for batch in pending_batches:
    batch_id = batch['BATCHID']
    batch_start_date = datetime.strftime(batch['BATCH_START_DATE'], "%d-%b-%Y %H:%M:%S.%f")
    batch_end_date = datetime.strftime(batch['BATCH_END_DATE'], "%d-%b-%Y %H:%M:%S.%f")

    # Set up logging for this batch
    log_file = os.path.join(log_directory, f"batch_{batch_id}.log")
    logging.basicConfig(filename=log_file, level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info(f"Starting reconciliation for BATCHID={batch_id}")

    # Step 3: Load Data from Source and Target
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
        .option("dbtable", f"({sql_query})") \
        .option("user", oracle_user) \
        .option("password", oracle_password) \
        .option("driver", oracle_driver) \
        .load()

    target_df = spark.read \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("collection", "target_collection") \
        .load() \
        .filter(f"migrationHistory.migrationBatchId = '{batch_id}'")

    # Step 4: Compare Source and Target Counts
    source_count = source_df.count()
    target_count = target_df.count()

    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_SRC_CNT = {source_count}, 
            RECONCILE_TGT_CNT = {target_count} 
        WHERE BATCHID = '{batch_id}'
    """
    cursor.execute(update_query)
    conn.commit()

    logger.info(f"Source Count: {source_count}, Target Count: {target_count}")

    # Step 5: Data Comparison
    def bson_to_str_uuid(bson_uuid):
        return str(bson_uuid)
    
    bson_to_str_uuid_udf = F.udf(bson_to_str_uuid, StringType())
    
    source_selected_df = source_df.select(
        F.col("Object_ID").alias("source_uuid"),
        F.col("DOMAIN").alias("source_DOMAIN"),
        F.col("DIVISION").alias("source_DIVISION"),
        F.col("SUBDIVISION").alias("source_SUBDIVISION")
    )
    
    target_selected_df = target_df.select(
        bson_to_str_uuid_udf(F.col("documentOS.documentOsGuid")).alias("target_uuid"),
        F.col("bankDocument.tenants.DOMAIN")[0].alias("target_DOMAIN"),
        F.col("bankDocument.tenants.DIVISION")[0].alias("target_DIVISION"),
        F.col("bankDocument.tenants.SUBDIVISION")[0].alias("target_SUBDIVISION")
    )
    
    comparison_df = source_selected_df.join(
        target_selected_df,
        source_selected_df["source_uuid"] == target_selected_df["target_uuid"],
        "inner"
    )
    
    mismatch_domain = F.when(F.col("source_DOMAIN") != F.col("target_DOMAIN"), F.lit("DOMAIN")).otherwise(None)
    mismatch_division = F.when(F.col("source_DIVISION") != F.col("target_DIVISION"), F.lit("DIVISION")).otherwise(None)
    mismatch_subdivision = F.when(F.col("source_SUBDIVISION") != F.col("target_SUBDIVISION"), F.lit("SUBDIVISION")).otherwise(None)
    
    mismatch_columns = F.array(mismatch_domain, mismatch_division, mismatch_subdivision)
    
    mismatch_df = comparison_df.withColumn("mismatch_columns", F.array_remove(mismatch_columns, None))
    
    mismatch_df = mismatch_df.filter(F.size(F.col("mismatch_columns")) > 0)
    
    reconcile_check = "MATCHED" if mismatch_df.count() == 0 else "MISMATCHED"
    
    logger.info(f"Data comparison result for BATCHID={batch_id}: {reconcile_check}")
    
    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_CHECK = '{reconcile_check}' 
        WHERE BATCHID = '{batch_id}'
    """
    cursor.execute(update_query)
    conn.commit()

    # Step 6: Retrieve and Validate GUIDs
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    
    def hcp_search_and_download_object(hcp_url, object_guid, auth_code, save_path):
        url = f"{hcp_url}/rest/objects/{object_guid}"
        try:
            response = requests.get(url, headers={'Authorization': auth_code}, verify=False)
            if response.status_code == 200 and response.content:
                os.makedirs(os.path.dirname(save_path), exist_ok=True)
                with open(save_path, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                return f"{object_guid}:SUCCESS"
            elif response.status_code == 404:
                return f"{object_guid}:FAILURE"
            else:
                return f"{object_guid}:FAILURE"
        except requests.exceptions.RequestException:
            return f"{object_guid}:FAILURE"
    
    def validate_storage_paths(batch_df):
        results = []
        for row in batch_df.collect():
            object_guid = row['documentOsGuid']
            save_path = f"{save_directory}/downloaded_object_{object_guid}.dat"
            status = hcp_search_and_download_object(hcp_url, object_guid, auth_code, save_path)
            results.append(status)
        return ",".join(results)
    
    guid_df = target_df.select("documentOsGuid", "storage.documentStoreUrl").limit(100)
    
    retrieval_validation_result = validate_storage_paths(guid_df)
    
    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RETRIEVAL_VALIDATION = '{retrieval_validation_result}'
        WHERE BATCH_ID = '{batch_id}'
    """
    cursor.execute(update_query)
    conn.commit()

    logger.info(f"GUID Retrieval Validation for BATCHID={batch_id}: {retrieval_validation_result}")

    # Step 7: Mark Reconciliation Completion
    current_time = datetime.now().strftime("%d-%b-%Y %H:%M:%S.%f")
    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_DTTM = TO_TIMESTAMP('{current_time}', 'DD-MON-YYYY HH24:MI:SS.FF6') 
        WHERE BATCHID = '{batch_id}'
    """
    cursor.execute(update_query)
    conn.commit()

    logger.info(f"Reconciliation completed for BATCHID={batch_id}")

# Close Oracle connection
cursor.close()
conn.close()

# Stop Spark session
spark.stop()
-----------
[oracle]
url = your_oracle_db_url
user = your_oracle_user
password = your_oracle_password
driver = oracle.jdbc.driver.OracleDriver

[mongodb]
uri = your_mongo_uri

[hcp]
url = https://your-hcp-url
auth_code = Basic your_encoded_auth_code

[paths]
log_directory = /path/to/log/directory
save_directory = /desired/path/to/save
