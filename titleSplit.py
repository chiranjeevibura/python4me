from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, substring_index

# Initialize Spark session
spark = SparkSession.builder \
                    .appName("Document Title Processing") \
                    .getOrCreate()

# Sample DataFrame
data = [
    ("doc1.test.title.poc.txt", "text/plain"),
    ("document_without_period", "application/pdf"),
    (None, "image/jpeg"),
    ("doc2.another.title.pptx", "application/vnd.ms-powerpoint")
]
columns = ["documentTitle", "mimeType"]

df = spark.createDataFrame(data, columns)

# Define a function to map mimeType to extension
def map_mime_type(mimeType):
    mime_map = {
        "text/plain": "txt",
        "application/pdf": "pdf",
        "image/jpeg": "jpg",
        "application/vnd.ms-powerpoint": "pptx"
    }
    return mime_map.get(mimeType, None)

# Register the UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

mime_type_udf = udf(map_mime_type, StringType())

# Apply the transformations
result_df = df.withColumn(
    "documentBaseName",
    when(col("documentTitle").isNotNull(), substring_index(col("documentTitle"), ".", -1)).otherwise(None)
).withColumn(
    "documentExtension",
    when(
        col("documentTitle").isNotNull() & (substring_index(col("documentTitle"), ".", -1) != col("documentTitle")),
        substring_index(col("documentTitle"), ".", -1)
    ).when(
        col("documentTitle").isNotNull() & (substring_index(col("documentTitle"), ".", -1) == col("documentTitle")),
        mime_type_udf(col("mimeType"))
    ).otherwise(mime_type_udf(col("mimeType")))
).withColumn(
    "documentBaseName",
    when(
        col("documentTitle").isNotNull(),
        substring_index(col("documentTitle"), ".", -1)
    ).otherwise(None)
)

# Show the results
result_df.show(truncate=False)

# Stop Spark session
spark.stop()
