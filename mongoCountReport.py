from pymongo import MongoClient
import pandas as pd

# MongoDB connection details
MONGO_CONFIG = {
    "uri": "mongodb://your_mongo_user:your_mongo_password@host:port",
    "database": "your_database",
    "collection": "your_collection"
}

# Fields for which to count unique values
FIELDS_TO_COUNT = [
    "mimeType",
    "primRegion",
    "primDomain",
    "docutatus",
    "abctup.tupleId",
    "abcDocument.Tenants.domain"
    # Add more fields as needed
]

# Connect to MongoDB
client = MongoClient(MONGO_CONFIG["uri"])
db = client[MONGO_CONFIG["database"]]
collection = db[MONGO_CONFIG["collection"]]

def get_unique_field_counts(field):
    """
    Aggregates unique values for a specified field and counts their occurrences.
    """
    pipeline = [
        {"$project": {field: 1}},
        {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}  # Sort by count, descending
    ]
    # Run aggregation
    results = collection.aggregate(pipeline)
    return [{"value": result["_id"], "count": result["count"]} for result in results]

# Collect and structure counts for each field
field_counts = {}
for field in FIELDS_TO_COUNT:
    counts = get_unique_field_counts(field)
    field_counts[field] = counts

# Convert to DataFrame for easy visualization
report_data = []
for field, counts in field_counts.items():
    for count in counts:
        report_data.append({"Field": field, "Value": count["value"], "Count": count["count"]})

# Generate report as DataFrame and save to CSV
report_df = pd.DataFrame(report_data)
report_df.to_csv("unique_field_counts_report.csv", index=False)
print(report_df)

# Close MongoDB connection
client.close()
