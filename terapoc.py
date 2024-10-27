from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    try:
        # Initialize Spark Session
        print("Initializing Spark Session...")
        spark = SparkSession.builder \
            .appName("Oracle_Teradata_Data_Integration") \
            .config("spark.jars", "/path_to_jdbc_driver/terajdbc4.jar,/path_to_jdbc_driver/tdgssconfig.jar,/path_to_jdbc_driver/ojdbc8.jar") \
            .getOrCreate()
        print("Spark Session initialized successfully.")

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

        # Step 1: Query Oracle DB and store result set into ora_main_df
        try:
            print("Fetching data from Oracle table ECMI_BUCKEYE...")
            oracle_query = "(SELECT app_ref_no FROM ecmi_buckeye) AS oracle_data"
            ora_main_df = spark.read.jdbc(
                oracle_jdbc_url,
                oracle_query,
                properties=oracle_properties
            )
            print("Data fetched from Oracle table ECMI_BUCKEYE.")
        except Exception as e:
            print(f"Error fetching data from Oracle: {e}")
            raise

        # Step 2: Query Teradata DB and store result set into tera_main_df
        try:
            print("Fetching data from Teradata table APP_CUST_REF_AUD...")
            teradata_query = """
            (SELECT app_ref_no, prod_acct_no, prod_cd FROM vcard_cns_us.app_cust_ref_aud) AS teradata_data
            """
            tera_main_df = spark.read.jdbc(
                teradata_jdbc_url,
                teradata_query,
                properties=teradata_properties
            )
            print("Data fetched from Teradata table APP_CUST_REF_AUD.")
        except Exception as e:
            print(f"Error fetching data from Teradata: {e}")
            raise

        # Step 3: Join dataframes where app_ref_no matches
        try:
            print("Joining Oracle and Teradata dataframes on app_ref_no...")
            tera_df_1 = tera_main_df.join(ora_main_df, "app_ref_no")
            print("Join successful. Number of matching records:", tera_df_1.count())
        except Exception as e:
            print(f"Error joining dataframes: {e}")
            raise

        # Step 4: Filter the dataframe where prod_cd = 'cc'
        try:
            print("Filtering dataframe where prod_cd = 'cc'...")
            tera_df_CC = tera_df_1.filter(col("prod_cd") == "cc")
            print("Filtered records where prod_cd = 'cc':", tera_df_CC.count())
        except Exception as e:
            print(f"Error filtering for prod_cd 'cc': {e}")
            raise

        # Step 5: Filter the dataframe where prod_cd = 'sb'
        try:
            print("Filtering dataframe where prod_cd = 'sb'...")
            tera_df_SB = tera_df_1.filter(col("prod_cd") == "sb")
            print("Filtered records where prod_cd = 'sb':", tera_df_SB.count())
        except Exception as e:
            print(f"Error filtering for prod_cd 'sb': {e}")
            raise

        # Step 6: Query Teradata DB and store result set into tera_df_2
        try:
            print("Fetching data from Teradata table ACCT_MED_CHG...")
            teradata_query_2 = """
            (SELECT prod_acct_no, statc_closed_maint_dt FROM vcard_cns_us.acct_med_chg) AS teradata_data_2
            """
            tera_df_2 = spark.read.jdbc(
                teradata_jdbc_url,
                teradata_query_2,
                properties=teradata_properties
            )
            print("Data fetched from Teradata table ACCT_MED_CHG.")
        except Exception as e:
            print(f"Error fetching data from Teradata ACCT_MED_CHG: {e}")
            raise

        # Step 7: Join tera_df_CC with tera_df_2 on prod_acct_no
        try:
            print("Joining tera_df_CC with tera_df_2 on prod_acct_no...")
            tera_df_cc_final = tera_df_CC.join(tera_df_2, "prod_acct_no") \
                .select("app_ref_no", "prod_acct_no", "statc_closed_maint_dt")
            print("Join successful. Number of matching records:", tera_df_cc_final.count())
        except Exception as e:
            print(f"Error joining tera_df_CC with tera_df_2: {e}")
            raise

        # Step 8: Query Teradata DB and store result set into tera_df_3
        try:
            print("Fetching data from Teradata table ACCT_STAT_CT...")
            teradata_query_3 = """
            (SELECT acct_id, cls_dt, cur_bal_am FROM vcard_smb_na.acct_stat_ct) AS teradata_data_3
            """
            tera_df_3 = spark.read.jdbc(
                teradata_jdbc_url,
                teradata_query_3,
                properties=teradata_properties
            )
            print("Data fetched from Teradata table ACCT_STAT_CT.")
        except Exception as e:
            print(f"Error fetching data from Teradata ACCT_STAT_CT: {e}")
            raise

        # Step 9: Filter tera_df_3 where cur_bal_am != 0
        try:
            print("Filtering tera_df_3 where cur_bal_am != 0...")
            tera_df_3_filtered = tera_df_3.filter(col("cur_bal_am") != 0)
            print("Filtered records where cur_bal_am != 0:", tera_df_3_filtered.count())
        except Exception as e:
            print(f"Error filtering tera_df_3 for cur_bal_am != 0: {e}")
            raise

        # Step 10: Join tera_df_SB with tera_df_3_filtered on prod_acct_no = acct_id
        try:
            print("Joining tera_df_SB with tera_df_3_filtered on prod_acct_no = acct_id...")
            tera_df_sb_final = tera_df_SB.join(tera_df_3_filtered, tera_df_SB.prod_acct_no == tera_df_3_filtered.acct_id) \
                .select("app_ref_no", "acct_id", "cls_dt")
            print("Join successful. Number of matching records:", tera_df_sb_final.count())
        except Exception as e:
            print(f"Error joining tera_df_SB with tera_df_3_filtered: {e}")
            raise

        # Step 11: Print the first 10 rows of tera_df_cc_final
        print("First 10 rows of tera_df_cc_final:")
        tera_df_cc_final.show(10, truncate=False)

        # Step 12: Print the first 10 rows of tera_df_sb_final
        print("First 10 rows of tera_df_sb_final:")
        tera_df_sb_final.show(10, truncate=False)

    except Exception as e:
        print(f"An error occurred during the execution: {e}")

    finally:
        # Stop Spark Session
        try:
            print("Stopping Spark Session...")
            spark.stop()
            print("Spark Session stopped successfully.")
        except Exception as e:
            print(f"Failed to stop Spark Session: {e}")

if __name__ == "__main__":
    main()
