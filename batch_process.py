import logging
import os
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, min as spark_min, max as spark_max, lit

# Set up logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, f"batch_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Starting the batch processing job.")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read and Write Data in Batches with Logging and UUIDs") \
    .getOrCreate()

# Connection properties for the source database
source_url = "jdbc:oracle:thin:@//source_host:1521/source_service_name"
source_properties = {
    "user": "source_user",
    "password": "source_password",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Read data from the source database, ensuring create_date is in TIMESTAMP format
df = spark.read.jdbc(
    url=source_url,
    table="(SELECT create_date, other_columns FROM docversion) t",  # Your SQL query
    properties=source_properties
)

# Sort the DataFrame by `create_date`
df_sorted = df.orderBy(col("create_date").cast("timestamp"))

# Add a row number to each row for batch splitting
window_spec = Window.orderBy("create_date")
df_with_row_num = df_sorted.withColumn("row_num", row_number().over(window_spec))

# Define the batch size
batch_size = 50000

# Calculate the number of batches
num_batches = df_with_row_num.count() // batch_size + 1
logger.info(f"Total number of batches to process: {num_batches}")

# Connection properties for the target database
target_url = "jdbc:oracle:thin:@//target_host:1521/target_service_name"
target_properties = {
    "user": "target_user",
    "password": "target_password",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Loop through each batch and write to the target database
for i in range(1, num_batches + 1):
    batch_df = df_with_row_num.filter((col("row_num") > (i-1) * batch_size) & (col("row_num") <= i * batch_size))
    
    # Drop the row_num column before writing
    batch_df = batch_df.drop("row_num")
    
    # Generate a unique UUID for the batch
    batch_id = str(uuid.uuid4())
    
    # Determine the start and end dates for the batch
    batch_start_date = batch_df.agg(spark_min("create_date").alias("batch_start_date")).collect()[0]["batch_start_date"]
    batch_end_date = batch_df.agg(spark_max("create_date").alias("batch_end_date")).collect()[0]["batch_end_date"]
    
    # Get the current timestamp for the batch creation date
    batch_create_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Log batch information
    logger.info(f"Processing batch {i}/{num_batches} with UUID: {batch_id}")
    logger.info(f"Batch Start Date: {batch_start_date}, Batch End Date: {batch_end_date}, Batch Create Date: {batch_create_date}")
    
    # Add batch metadata columns to the DataFrame
    batch_df = batch_df.withColumn("batch_id", lit(batch_id)) \
                       .withColumn("batch_start_date", lit(batch_start_date)) \
                       .withColumn("batch_end_date", lit(batch_end_date)) \
                       .withColumn("batch_create_date", lit(batch_create_date))
    
    # Write the batch to the target database
    batch_df.write.jdbc(url=target_url, table="target_table_name", mode="append", properties=target_properties)

    # Log completion of the batch
    logger.info(f"Batch {i}/{num_batches} with UUID: {batch_id} written to the target database.")

logger.info("Batch processing job completed successfully.")
spark.stop()
