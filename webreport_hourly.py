from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient
from datetime import datetime, timedelta
import json

app = Flask(__name__)

# MongoDB connection
client = MongoClient("mongodb://your_mongo_uri")
db = client['your_db_name']
collection = db['your_collection_name']

# Function to fetch report data from MongoDB
def fetch_report(origination_system, start_date, end_date):
    filter_condition = {
        "migrationHistory.migratedBy": {"$ne": "cdlmdbmp"},
        "bankDocument.tenants": {
            "$elemMatch": {
                "domain": "ABC",
                "division": "ABC"
            }
        },
        "systemCreatedDateTime": {"$gt": start_date, "$lt": end_date}
    }

    if origination_system != "ALL":
        filter_condition["adminDocument.originationSystemName"] = origination_system

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
    
    result = collection.aggregate(pipeline)
    return list(result)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_report', methods=['POST'])
def get_report():
    data = request.json
    origination_system = data['originationSystem']
    start_date = datetime.strptime(data['startDate'], "%Y-%m-%d")
    end_date = datetime.strptime(data['endDate'], "%Y-%m-%d") + timedelta(days=1)  # Add 1 day for inclusive end date
    report = fetch_report(origination_system, start_date, end_date)
    
    # Convert report to the format needed for frontend display
    report_data = []
    for entry in report:
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
    
    return jsonify(report_data)

if __name__ == "__main__":
    app.run(debug=True)



---------

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Document Count Report</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
    <div class="container mt-5">
        <h2>Document Count Report</h2>
        <form id="filterForm">
            <div class="row mb-3">
                <div class="col">
                    <label for="originationSystem">Origination System</label>
                    <select class="form-control" id="originationSystem">
                        <option value="ALL">ALL</option>
                        <option value="SystemA">SystemA</option>
                        <option value="SystemB">SystemB</option>
                    </select>
                </div>
                <div class="col">
                    <label for="startDate">Start Date</label>
                    <input type="date" class="form-control" id="startDate" value="2024-11-17">
                </div>
                <div class="col">
                    <label for="endDate">End Date</label>
                    <input type="date" class="form-control" id="endDate" value="2024-11-17">
                </div>
            </div>
            <button type="submit" class="btn btn-primary">Show Report</button>
        </form>

        <div id="reportSection" class="mt-5">
            <h3>Report</h3>
            <table class="table" id="reportTable">
                <thead>
                    <tr>
                        <th>Start Time</th>
                        <th>End Time</th>
                        <th>Overall Count</th>
                        <th>System Name</th>
                        <th>System Count</th>
                    </tr>
                </thead>
                <tbody></tbody>
            </table>

            <canvas id="reportChart"></canvas>
        </div>
    </div>

    <script>
        document.getElementById('filterForm').addEventListener('submit', function(e) {
            e.preventDefault();
            
            const originationSystem = document.getElementById('originationSystem').value;
            const startDate = document.getElementById('startDate').value;
            const endDate = document.getElementById('endDate').value;
            
            fetch('/get_report', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ originationSystem, startDate, endDate })
            })
            .then(response => response.json())
            .then(data => {
                const tableBody = document.querySelector('#reportTable tbody');
                tableBody.innerHTML = '';

                let labels = [];
                let overallCounts = [];
                let systemData = {};

                data.forEach(item => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>${item.start_time}</td>
                        <td>${item.end_time}</td>
                        <td>${item.overallCount}</td>
                        <td>${item.system_originationSystemName}</td>
                        <td>${item.system_count}</td>
                    `;
                    tableBody.appendChild(row);

                    labels.push(item.start_time);
                    overallCounts.push(item.overallCount);
                    if (!systemData[item.system_originationSystemName]) {
                        systemData[item.system_originationSystemName] = [];
                    }
                    systemData[item.system_originationSystemName].push(item.system_count);
                });

                // Chart.js visualization
                const ctx = document.getElementById('reportChart').getContext('2d');
                const chartData = {
                    labels: labels,
                    datasets: Object.keys(systemData).map(system => ({
                        label: system,
                        data: systemData[system],
                        borderColor: 'rgba(75, 192, 192, 1)',
                        fill: false
                    }))
                };

                new Chart(ctx, {
                    type: 'line',
                    data: chartData
                });
            });
        });
    </script>
</body>
</html>
