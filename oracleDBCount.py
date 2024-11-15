import cx_Oracle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to the Oracle Database
dsn = cx_Oracle.makedsn("your_host", "your_port", service_name="your_service_name")
connection = cx_Oracle.connect(user="your_username", password="your_password", dsn=dsn)

# List of example SQL queries
queries = {
    "employee_count_per_department": "SELECT department_id, employee_count FROM employees ORDER BY department_id",
    "monthly_sales": "SELECT month, total_sales FROM sales ORDER BY month"
}

# Loop over each query, save the result to CSV, and generate a chart
for query_name, query in queries.items():
    # Run the query and store the result in a pandas DataFrame
    df = pd.read_sql(query, connection)

    # Save the DataFrame to a CSV file
    csv_filename = f"{query_name}_result.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Results of {query_name} saved to {csv_filename}")

    # Generate a chart based on the DataFrame
    plt.figure(figsize=(10, 6))
    
    # Generate charts based on the type of data
    if query_name == "employee_count_per_department":
        sns.barplot(x="department_id", y="employee_count", data=df)
        plt.title("Employee Count per Department")
        plt.xlabel("Department ID")
        plt.ylabel("Employee Count")
    elif query_name == "monthly_sales":
        sns.lineplot(x="month", y="total_sales", data=df)
        plt.title("Monthly Sales")
        plt.xlabel("Month")
        plt.ylabel("Total Sales")
    
    # Save and show the chart
    chart_filename = f"{query_name}_chart.png"
    plt.savefig(chart_filename)
    print(f"Chart for {query_name} saved to {chart_filename}")
    plt.show()

# Close the database connection
connection.close()
