import cx_Oracle
import pandas as pd
from pymongo import MongoClient

# Oracle Connection
dsn = cx_Oracle.makedsn("your_host", "your_port", service_name="your_service_name")
oracle_connection = cx_Oracle.connect(user="your_username", password="your_password", dsn=dsn)

# MongoDB Connection
mongo_client = MongoClient("mongodb://your_mongo_host:your_port/")
mongo_db = mongo_client["your_mongo_database"]
mongo_collection = mongo_db["your_mongo_collection"]

# Queries and Fields Mapping
queries = {
    "department_count": {
        "oracle_query": "SELECT COUNT(*) AS count, department_id FROM employees GROUP BY department_id",
        "mongo_query": [
            {"$group": {"_id": "$department_id", "count": {"$sum": 1}}}
        ],
        "source_oracle_col_name": "department_id",
        "target_mongo_field_name": "department_id"
    },
    "monthly_sales_count": {
        "oracle_query": "SELECT COUNT(*) AS count, month FROM sales GROUP BY month",
        "mongo_query": [
            {"$group": {"_id": "$month", "count": {"$sum": 1}}}
        ],
        "source_oracle_col_name": "month",
        "target_mongo_field_name": "month"
    }
}

# Reconciliation Results
reconciliation_results = []

# Process Each Query
for query_name, query_details in queries.items():
    # Fetch Oracle Data
    oracle_df = pd.read_sql(query_details["oracle_query"], oracle_connection)
    oracle_count = oracle_df["count"].sum()

    # Fetch MongoDB Data
    mongo_aggregation_result = list(mongo_collection.aggregate(query_details["mongo_query"]))
    mongo_count = sum(item["count"] for item in mongo_aggregation_result)

    # Determine Status
    status = "matched" if oracle_count == mongo_count else "not matched"

    # Append Reconciliation Result
    reconciliation_results.append({
        "source_oracle_col_name": query_details["source_oracle_col_name"],
        "source_oracle_count": oracle_count,
        "target_mongo_field_name": query_details["target_mongo_field_name"],
        "target_mongo_count": mongo_count,
        "status": status
    })

# Generate Reconciliation Report
reconciliation_df = pd.DataFrame(reconciliation_results)
reconciliation_csv_filename = "reconciliation_report.csv"
reconciliation_df.to_csv(reconciliation_csv_filename, index=False)
print(f"Reconciliation report saved to {reconciliation_csv_filename}")

# Close Connections
oracle_connection.close()
mongo_client.close()
