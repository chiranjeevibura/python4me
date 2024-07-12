from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("OracleUpdateJob") \
    .config("spark.jars", "path/to/ojdbc8.jar") \
    .config("spark.executor.extraClassPath", "path/to/ojdbc8.jar") \
    .getOrCreate()

# Database connection properties
source_db_properties = {
    "user": "source_user",
    "password": "source_password",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

target_db_properties = {
    "user": "target_user",
    "password": "target_password",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# JDBC URLs
source_url = "jdbc:oracle:thin:@//source_host:source_port/source_service"
target_url = "jdbc:oracle:thin:@//target_host:target_port/target_service"

# Read from source table
source_df = spark.read.jdbc(url=source_url, table="source_table", properties=source_db_properties) \
    .select("account_number", "close_date")

# Read from target table
target_df = spark.read.jdbc(url=target_url, table="target_table", properties=target_db_properties) \
    .select("account_number", "close_date", "record_code")

# Join source and target DataFrames
joined_df = source_df.join(target_df, on="account_number", how="inner") \
    .select(source_df["account_number"], source_df["close_date"])

# Define the static record code
record_code_value = "STATIC_RECORD_CODE"

# Create the DataFrame to update
update_df = joined_df.withColumn("record_code", lit(record_code_value))

# Function to update target table
def update_target_table(batch_df, batch_id):
    batch_df.write.jdbc(
        url=target_url,
        table="target_table",
        mode="append",
        properties=target_db_properties
    )

# Write the DataFrame in batches to handle large data
update_df.writeStream \
    .foreachBatch(update_target_table) \
    .start() \
    .awaitTermination()

# Stop Spark session
spark.stop()
