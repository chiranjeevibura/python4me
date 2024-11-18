import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd

# MongoDB connection setup
client = MongoClient("mongodb://your_mongo_uri")
db = client['your_db_name']
collection = db['your_collection_name']

# Email setup
sender_email = "your_email@example.com"
receiver_email = "receiver@example.com"
smtp_server = "smtp.example.com"
smtp_port = 587
smtp_user = "your_email@example.com"
smtp_password = "your_email_password"

def generate_report():
    # Define the filter conditions
    filter_condition = {
        "migrationHistory.migratedBy": {"$ne": "cdlmdbmp"},
        "bankDocument.tenants": {
            "$elemMatch": {
                "domain": "ABC",
                "division": "ABC"
            }
        },
        "systemCreatedDateTime": {"$gt": datetime(2024, 11, 17, 0, 0, 0)}
    }

    # MongoDB aggregation pipeline
    pipeline = [
        {"$match": filter_condition},
        {
            "$addFields": {
                "sixHourBucket": {
                    "$dateToString": {
                        "format": "%Y-%m-%d %H:00:00",
                        "date": {
                            "$dateTrunc": {
                                "unit": "hour",
                                "binSize": 6,
                                "date": "$systemCreatedDateTime"
                            }
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "sixHourBucket": "$sixHourBucket",
                    "originationSystemName": "$adminDocument.originationSystemName"
                },
                "documentCount": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": "$_id.sixHourBucket",
                "overallCount": {"$sum": "$documentCount"},
                "systemCounts": {
                    "$push": {
                        "originationSystemName": "$_id.originationSystemName",
                        "count": "$documentCount"
                    }
                }
            }
        },
        {"$sort": {"_id": 1}}
    ]

    # Run the aggregation
    result = collection.aggregate(pipeline)

    # Convert result to a DataFrame for easier manipulation
    report_data = []
    for entry in result:
        six_hour_bucket = entry['_id']
        overall_count = entry['overallCount']
        for system in entry['systemCounts']:
            system_name = system['originationSystemName']
            system_count = system['count']
            report_data.append({
                "start_time": six_hour_bucket,
                "end_time": (datetime.strptime(six_hour_bucket, "%Y-%m-%d %H:%M:%S") + timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S"),
                "overallCount": overall_count,
                "system_originationSystemName": system_name,
                "system_count": system_count
            })

    # Create a DataFrame for the report
    df = pd.DataFrame(report_data)

    # Format the DataFrame to table-like format
    report_html = df.to_html(index=False, header=True)

    # Construct email body
    email_body = f"""
    <html>
    <body>
        <h2>Document Count Report - 6 Hour Interval</h2>
        <p>This report shows the number of documents added every 6 hours since the system went live, including the overall count and system-wise count for each interval.</p>
        {report_html}
        <p><b>Notes:</b><br>
        The overall count represents the total document count for each 6-hour interval, and the system-wise counts show how many documents were added by each origination system during the same interval.</p>
        <p>End of Report</p>
    </body>
    </html>
    """

    # Create the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = 'Document Count Report - 6 Hour Interval'
    msg.attach(MIMEText(email_body, 'html'))

    # Send the email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")

if __name__ == "__main__":
    generate_report()
