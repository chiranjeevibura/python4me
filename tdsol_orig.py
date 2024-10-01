Workflow
Step 1: Load 17 million records (APP_REF_NO) from Oracle Database ECMI_BUCKEYE.
Step 2: Extract APP_REF_NO, PROD_ACCT_NO, and PROD_CD from Teradata table VCARD_CNS_US.APP_CUST_REF_AUD for all APP_REF_NO found in Oracle. Filter on PROD_CD values: CC for Consumer and SB for Small Business.
Step 3: For Consumer (PROD_CD = CC), extract APP_REF_NO, PROD_ACCT_NO, and STATC_CLOSED_MAINT_DT from Teradata table VCARD_CNS_US.ACCT_MED_CHG where PROD_ACCT_NO matches.
Step 4: For Small Business (PROD_CD = SB), extract ACCT_ID, APP_REF_NO, CUR_BL_AM, and CLS_DT from Teradata table VCARD_SMB_NA.ACCT_STAT_CT where ACCT_ID matches and filter out records where CUR_BL_AM is not zero.


Explanation of the Updated Code
Step 1: Load 17 Million Records from Oracle:

Reads APP_REF_NO from the Oracle database (CE2OS1.ECMI_BUCKEYE) and caches the data to be used in the subsequent join.
Step 2: Extract APP_REF_NO, PROD_ACCT_NO, and PROD_CD from Teradata:

Loads data from VCARD_CNS_US.APP_CUST_REF_AUD in Teradata and performs an inner join with the Oracle APP_REF_NO.
The joined result is filtered based on PROD_CD: CC for Consumer accounts and SB for Small Business accounts.
Step 3: Process Consumer Data:

For Consumer accounts, the program queries VCARD_CNS_US.ACCT_MED_CHG in Teradata to fetch STATC_CLOSED_MAINT_DT for the matched PROD_ACCT_NO.
The result is stored as consumer_final_df.
Step 4: Process Small Business Data:

For Small Business accounts, the program queries VCARD_SMB_NA.ACCT_STAT_CT in Teradata to fetch CUR_BL_AM and CLS_DT.
Filters out records where CUR_BL_AM != 0, as those accounts should not be closed. The remaining records are stored in small_business_final_df.
Step 5: Save Results:

The final results are written to CSV files or can be stored back into the Oracle database if needed.
Performance Considerations:
Broadcast Join: The 17 million APP_REF_NO records from Oracle are broadcasted to ensure an efficient join with the Teradata tables, reducing shuffling.
Caching: The matched APP_REF_NO, PROD_ACCT_NO, and PROD_CD are cached as they are reused in the consumer and small business logic.
Partitioning: The program leverages Spark’s default partitioning, but it can be adjusted based on the infrastructure and data volume.
This updated logic matches your clarified requirements while ensuring performance for handling large datasets across Oracle and Teradata databases.


from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Oracle_Teradata_Integration") \
    .config("spark.jars", "/path_to_jdbc_driver/terajdbc4.jar,/path_to_jdbc_driver/tdgssconfig.jar,/path_to_jdbc_driver/ojdbc8.jar") \
    .getOrCreate()

# Oracle JDBC connection details
oracle_jdbc_url = "jdbc:oracle:thin:@<oracle_host>:<port>/<service_name>"
oracle_properties = {
    "user": "<username>",
    "password": "<password>",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Step 1: Read 17 million records from Oracle
oracle_query = "(SELECT APP_REF_NO FROM CE2OS1.ECMI_BUCKEYE) AS oracle_data"
oracle_df = spark.read.jdbc(oracle_jdbc_url, oracle_query, properties=oracle_properties)

# Teradata JDBC connection details
teradata_jdbc_url = "jdbc:teradata://<teradata_host>"
teradata_properties = {
    "user": "<username>",
    "password": "<password>"
}

# Step 2: Load data from Teradata: Extract APP_REF_NO, PROD_ACCT_NO, and PROD_CD from VCARD_CNS_US.APP_CUST_REF_AUD
cust_ref_query = "(SELECT APP_REF_NO, PROD_ACCT_NO, PROD_CD FROM VCARD_CNS_US.APP_CUST_REF_AUD) AS cust_ref_data"
cust_ref_df = spark.read.jdbc(teradata_jdbc_url, cust_ref_query, properties=teradata_properties)

# Join Oracle data (APP_REF_NO) with Teradata's APP_CUST_REF_AUD
oracle_broadcast = broadcast(oracle_df)
matched_df = cust_ref_df.join(oracle_broadcast, cust_ref_df.APP_REF_NO == oracle_df.APP_REF_NO).select(
    cust_ref_df.APP_REF_NO, cust_ref_df.PROD_ACCT_NO, cust_ref_df.PROD_CD)

# Cache matched records since we will reuse them
matched_df.cache()

# Step 3: Filter Consumer accounts (PROD_CD = 'CC') and get STATC_CLOSED_MAINT_DT from ACCT_MED_CHG
consumer_df = matched_df.filter(matched_df.PROD_CD == 'CC').select("APP_REF_NO", "PROD_ACCT_NO")

# Query from Teradata's ACCT_MED_CHG for Consumer accounts
acct_med_chg_query = "(SELECT PROD_ACCT_NO, STATC_CLOSED_MAINT_DT FROM VCARD_CNS_US.ACCT_MED_CHG) AS acct_med_chg_data"
acct_med_chg_df = spark.read.jdbc(teradata_jdbc_url, acct_med_chg_query, properties=teradata_properties)

# Join with Consumer accounts on PROD_ACCT_NO
consumer_final_df = consumer_df.join(acct_med_chg_df, "PROD_ACCT_NO").select(
    "APP_REF_NO", "PROD_ACCT_NO", "STATC_CLOSED_MAINT_DT")

# Step 4: Filter Small Business accounts (PROD_CD = 'SB') and get ACCT_STAT_CT data
small_business_df = matched_df.filter(matched_df.PROD_CD == 'SB').select("APP_REF_NO", "PROD_ACCT_NO")

# Query from Teradata's ACCT_STAT_CT for Small Business accounts
acct_stat_ct_query = "(SELECT ACCT_ID, CUR_BL_AM, CLS_DT FROM VCARD_SMB_NA.ACCT_STAT_CT) AS acct_stat_ct_data"
acct_stat_ct_df = spark.read.jdbc(teradata_jdbc_url, acct_stat_ct_query, properties=teradata_properties)

# Join with Small Business accounts on ACCT_ID (which is PROD_ACCT_NO)
small_business_final_df = small_business_df.join(acct_stat_ct_df, small_business_df.PROD_ACCT_NO == acct_stat_ct_df.ACCT_ID)

# Step 4: Apply CUR_BL_AM check, filter out records where CUR_BL_AM is not zero
small_business_final_df = small_business_final_df.filter(small_business_final_df.CUR_BL_AM == 0).select(
    "APP_REF_NO", "ACCT_ID", "CLS_DT")

# Step 5: Save results or write them to the destination
consumer_final_df.write.format("csv").save("/path_to_output/consumer_results")
small_business_final_df.write.format("csv").save("/path_to_output/small_business_results")

# Alternatively, you can write results back to Oracle or Teradata table
consumer_final_df.write.jdbc(oracle_jdbc_url, "consumer_results_table", mode="overwrite", properties=oracle_properties)
small_business_final_df.write.jdbc(oracle_jdbc_url, "small_business_results_table", mode="overwrite", properties=oracle_properties)

spark.stop()
