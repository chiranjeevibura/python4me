from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import traceback

def main():
    try:
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Oracle_Teradata_Data_Integration") \
            .config("spark.jars", "/path_to_jdbc_driver/terajdbc4.jar,/path_to_jdbc_driver/tdgssconfig.jar,/path_to_jdbc_driver/ojdbc8.jar") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()

        # Oracle JDBC connection details
        oracle_jdbc_url = "jdbc:oracle:thin:@<oracle_host>:<port>/<service_name>"
        oracle_properties = {
            "user": "<oracle_username>",
            "password": "<oracle_password>",
            "driver": "oracle.jdbc.driver.OracleDriver"
        }

        # Teradata JDBC connection details
        teradata_jdbc_url = "jdbc:teradata://<teradata_host>/LOGMECH=LDAP"
        teradata_properties = {
            "user": "<teradata_username>",
            "password": "<teradata_password>",
            "driver": "com.teradata.jdbc.TeraDriver"
        }

        # Step 1: Query Oracle and store in ora_main_df
        ora_main_df = spark.read.jdbc(
            oracle_jdbc_url,
            "(SELECT app_ref_no FROM ecmi_buckeye) AS oracle_data",
            properties=oracle_properties
        ).repartition(200, col("app_ref_no"))

        # Step 2: Join with Teradata APP_CUST_REF_AUD
        tera_main_df = spark.read.jdbc(
            teradata_jdbc_url,
            "(SELECT app_ref_no, prod_acct_no, prod_cd FROM vcard_cns_us.app_cust_ref_aud) AS teradata_data",
            properties=teradata_properties
        ).repartition(200, col("app_ref_no"))

        joined_df_1 = ora_main_df.join(tera_main_df, "app_ref_no", "inner") \
            .select("app_ref_no", "prod_acct_no", "prod_cd")

        # Step 3: Filter joined_df_1
        filtered_cc_df = joined_df_1.filter(col("prod_cd") == "cc")
        filtered_sb_df = joined_df_1.filter(col("prod_cd") == "sb")

        # Step 4: Join with ACCT_MED_CHG on prod_acct_no
        acct_med_df = spark.read.jdbc(
            teradata_jdbc_url,
            "(SELECT prod_acct_no, statc_closed_maint_dt FROM vcard_cns_us.acct_med_chg) AS teradata_data_2",
            properties=teradata_properties
        ).repartition(200, col("prod_acct_no"))

        final_cc_df = filtered_cc_df.join(acct_med_df, "prod_acct_no", "inner") \
            .select("app_ref_no", "prod_acct_no", "statc_closed_maint_dt")

        # Step 5: Join with ACCT_STAT_CT
        acct_stat_df = spark.read.jdbc(
            teradata_jdbc_url,
            "(SELECT acct_id, cls_dt, cur_bal_am FROM vcard_smb_na.acct_stat_ct) AS teradata_data_3",
            properties=teradata_properties
        ).filter(col("cur_bal_am") != 0) \
        .repartition(200, col("acct_id"))

        final_sb_df = filtered_sb_df.join(acct_stat_df, filtered_sb_df.prod_acct_no == acct_stat_df.acct_id, "inner") \
            .select("app_ref_no", "prod_acct_no", "cls_dt")

        # Display results
        final_cc_df.show(10, truncate=False)
        final_sb_df.show(10, truncate=False)

    except Exception as e:
        print(f"Error during execution: {traceback.format_exc()}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
