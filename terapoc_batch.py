from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import traceback

def process_batch(spark, app_ref_no_batch):
    try:
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

        # Step 1: Query Oracle for the specific batch of app_ref_no
        ora_main_df = spark.read.jdbc(
            oracle_jdbc_url,
            f"(SELECT app_ref_no FROM abc_bckeye WHERE app_ref_no BETWEEN {app_ref_no_batch[0]} AND {app_ref_no_batch[1]}) AS oracle_data",
            properties=oracle_properties
        )

        # Step 2: Join with Teradata APP_CUST_AUD
        tera_main_df = spark.read.jdbc(
            teradata_jdbc_url,
            "(SELECT app_ref_no, prod_acct_no, prod_cd FROM vcard_cns_us.app_cust_aud) AS teradata_data",
            properties=teradata_properties
        )

        joined_df_1 = ora_main_df.join(tera_main_df, "app_ref_no", "inner") \
            .select("app_ref_no", "prod_acct_no", "prod_cd")

        # Step 3: Filter joined_df_1
        filtered_cc_df = joined_df_1.filter(col("prod_cd") == "cc")
        filtered_sb_df = joined_df_1.filter(col("prod_cd") == "sb")

        # Step 4: Join with ACCT_MED_CHG on prod_acct_no
        acct_med_df = spark.read.jdbc(
            teradata_jdbc_url,
            "(SELECT prod_acct_no, statc_closed_maint_dt FROM vcard_cns_us.acct_chg) AS teradata_data_2",
            properties=teradata_properties
        )

        final_cc_df = filtered_cc_df.join(acct_med_df, "prod_acct_no", "inner") \
            .select("app_ref_no", "prod_acct_no", "statc_closed_maint_dt")

        # Step 5: Join with ACCT_STAT_CT
        acct_stat_df = spark.read.jdbc(
            teradata_jdbc_url,
            "(SELECT acct_id, cls_dt, cur_bal_am FROM vcard_smb_na.acct_stat_ct) AS teradata_data_3",
            properties=teradata_properties
        ).filter(col("cur_bal_am") != 0)

        final_sb_df = filtered_sb_df.join(acct_stat_df, filtered_sb_df.prod_acct_no == acct_stat_df.acct_id, "inner") \
            .select("app_ref_no", "prod_acct_no", "cls_dt")

        # Returning results from this batch
        return final_cc_df, final_sb_df

    except Exception as e:
        print(f"Error during batch processing {app_ref_no_batch}: {traceback.format_exc()}")
        return None, None


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

        # Step 1: Get total range of app_ref_no from DUCKEYE
        total_range_df = spark.read.jdbc(
            "jdbc:oracle:thin:@<oracle_host>:<port>/<service_name>",
            "(SELECT MIN(app_ref_no) AS min_ref_no, MAX(app_ref_no) AS max_ref_no FROM abc_duckeye) AS range_data",
            properties={
                "user": "<oracle_username>",
                "password": "<oracle_password>",
                "driver": "oracle.jdbc.driver.OracleDriver"
            }
        ).first()

        min_ref_no = total_range_df[0]
        max_ref_no = total_range_df[1]

        # Step 2: Define batch size
        batch_size = 1000000  # Process 1 million records at a time
        batches = [(start, start + batch_size - 1) for start in range(min_ref_no, max_ref_no + 1, batch_size)]

        final_cc_results = []
        final_sb_results = []

        # Step 3: Process each batch
        for app_ref_no_batch in batches:
            print(f"Processing batch: {app_ref_no_batch}")
            final_cc_df, final_sb_df = process_batch(spark, app_ref_no_batch)

            if final_cc_df is not None:
                final_cc_results.append(final_cc_df)

            if final_sb_df is not None:
                final_sb_results.append(final_sb_df)

        # Step 4: Combine results from all batches
        if final_cc_results:
            combined_cc_df = final_cc_results[0]
            for df in final_cc_results[1:]:
                combined_cc_df = combined_cc_df.union(df)

            combined_cc_df.show(10, truncate=False)

        if final_sb_results:
            combined_sb_df = final_sb_results[0]
            for df in final_sb_results[1:]:
                combined_sb_df = combined_sb_df.union(df)

            combined_sb_df.show(10, truncate=False)

    except Exception as e:
        print(f"Error during execution: {traceback.format_exc()}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
