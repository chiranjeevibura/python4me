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
    .option("driver", oracle_driver) \
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
        .filter(f"migrationHistory.migrationBatchId = '{batch['BATCHID']}'")

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
    # UDF to convert BSON UUID to string
    def bson_to_str_uuid(bson_uuid):
        return str(bson_uuid)
    
    bson_to_str_uuid_udf = F.udf(bson_to_str_uuid, StringType())
    
    # Select and alias relevant columns from the source and target DataFrames
    source_selected_df = source_df.select(
        F.col("Object_ID").alias("source_uuid"),
        F.col("DOMAIN").alias("source_DOMAIN"),
        F.col("DIVISION").alias("source_DIVISION"),
        F.col("SUBDIVISION").alias("source_SUBDIVISION")
    )
    
    # Convert array to string in the target DataFrame
    target_selected_df = target_df.select(
        bson_to_str_uuid_udf(F.col("documentOS.documentOsGuid")).alias("target_uuid"),
        F.col("bankDocument.tenants.DOMAIN")[0].alias("target_DOMAIN"),  # Extract first element as string
        F.col("bankDocument.tenants.DIVISION")[0].alias("target_DIVISION"),  # Extract first element as string
        F.col("bankDocument.tenants.SUBDIVISION")[0].alias("target_SUBDIVISION")  # Extract first element as string
    )
    
    # Join the DataFrames on the UUID
    comparison_df = source_selected_df.join(
        target_selected_df,
        source_selected_df["source_uuid"] == target_selected_df["target_uuid"],
        "inner"
    )
    
    # Compare the fields and track mismatches
    mismatch_domain = F.when(F.col("source_DOMAIN") != F.col("target_DOMAIN"), F.lit("DOMAIN")).otherwise(None)
    mismatch_division = F.when(F.col("source_DIVISION") != F.col("target_DIVISION"), F.lit("DIVISION")).otherwise(None)
    mismatch_subdivision = F.when(F.col("source_SUBDIVISION") != F.col("target_SUBDIVISION"), F.lit("SUBDIVISION")).otherwise(None)
    
    # Create an array column to capture the mismatches
    mismatch_columns = F.array(
        mismatch_domain, mismatch_division, mismatch_subdivision
    )
    
    # Filter out nulls (i.e., no mismatches) and flatten the array into a single column
    mismatch_df = comparison_df.withColumn("mismatch_columns", F.array_remove(mismatch_columns, None))
    
    # Filter rows where there are mismatches
    mismatch_df = mismatch_df.filter(F.size(F.col("mismatch_columns")) > 0)
    
    # Determine the reconciliation result
    reconcile_check = "MATCHED" if mismatch_df.count() == 0 else "MISMATCHED"
    
    # Log the result
    logger.info(f"Data comparison result for BATCHID={batch['BATCHID']}: {reconcile_check}")
    
    # Update the reconciliation status in the batch table
    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RECONCILE_CHECK = '{reconcile_check}' 
        WHERE BATCHID = '{batch['BATCHID']}'
    """
    cursor.execute(update_query)
    conn.commit()

    # Step 3.3: Retrieve and Validate GUIDs
    import requests
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    import os
    import shutil
    
    # Disable SSL warnings (equivalent to NoopHostnameVerifier in Java)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    
    # Function to search and download an object from HCP
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
        except requests.exceptions.RequestException as e:
            return f"{object_guid}:FAILURE"
    
    # Function to validate storage paths for a batch of GUIDs
    def validate_storage_paths(batch_df):
        results = []
        for row in batch_df.collect():
            object_guid = row['documentOsGuid']
            save_path = f"/desired/path/to/save/downloaded_object_{object_guid}.dat"
            status = hcp_search_and_download_object(hcp_url, object_guid, auth_code, save_path)
            results.append(status)
        return ",".join(results)
    
    # Your HCP details
    hcp_url = "https://your-hcp-url"
    auth_code = "Basic your_encoded_auth_code"
    
    # Sample target DataFrame
    guid_df = target_df.select("documentOsGuid", "storage.documentStoreUrl").limit(100)
    
    # Validate storage paths and collect results
    retrieval_validation_result = validate_storage_paths(guid_df)
    
    # Update the batch table with validation results (once for the entire batch)
    update_query = f"""
        UPDATE SparkReconBatchTable 
        SET RETRIEVAL_VALIDATION = '{retrieval_validation_result}'
        WHERE BATCH_ID = '{batch_id}'
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

# Clean up: Delete all downloaded data under save_path
import shutil

def delete_downloaded_files(directory_path):
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
        print(f"Deleted all downloaded files from {directory_path}")

# Assuming save_path used during download
delete_downloaded_files("/desired/path/to/save")

print("Reconciliation and cleanup completed.")

# Stop Spark session
spark.stop()
