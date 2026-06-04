import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
import pandas as pd 

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
mysql_db_params = {
    "dbname": os.getenv("mysql_amt_replica_db_name"),
    "user": os.getenv("mysql_amt_replica_db_user"),
    "password": os.getenv("mysql_amt_replica_db_password"),
    "host": os.getenv("mysql_amt_replica_db_host"),
    "port": os.getenv("mysql_db_port") 
}

# Establish connection to MySQL db
def establish_mysql_db_connection():
    try:
        connection = mysql.connector.connect(
            database=mysql_db_params["dbname"],
            user=mysql_db_params["user"],
            password=mysql_db_params["password"],
            host=mysql_db_params["host"],
            port=mysql_db_params["port"]
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

# Define fields for each of the database table
mysql_db_table_fields = {
    "customers": [
        "createdAt",
        "id",
        "name",
        "phoneNumber",
        "createdBy"
    ]
}

# Fetch data from MySQL db
def fetch_mysql_data(table_name, fields):
    connection = establish_mysql_db_connection()
    if connection:
        try:
            cursor = connection.cursor()
            query = f"SELECT {', '.join(fields)} FROM {table_name}"
            cursor.execute(query)
            rows = cursor.fetchall()
            print(f"Number of records fetched: {len(rows)}")  # Print number of records fetched
            return rows
        except Error as e:
            print(f"Error fetching data from MySQL: {e}")
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")
    return None

# Save data to Excel
def save_to_excel(data, fields, filename="output.xlsx"):
    try:
        # Convert the data into a pandas DataFrame
        df = pd.DataFrame(data, columns=fields)
        # Save the DataFrame to an Excel file
        df.to_excel(filename, index=False)
        print(f"Data saved to {filename}")
    except Exception as e:
        print(f"Error saving data to Excel: {e}")

def main():
    table_name = "customers"
    fields = mysql_db_table_fields[table_name]
    data = fetch_mysql_data(table_name, fields)
    if data:
        # Save the fetched data to an Excel file
        save_to_excel(data, fields, filename="mysql-data-excel-v1.xlsx")

# Execute the main function
if __name__ == "__main__":
    main()