import os 
import mysql.connector

# Database connection parameters
amt_replica_mysql_db_params = {
    "database": os.getenv("amt_replica_mysql_db"),
    "user": os.getenv("amt_replica_mysql_db_user"),
    "password": os.getenv("amt_replica_mysql_db_password"),
    "host": os.getenv("amt_replica_mysql_db_host"),
    "port": os.getenv("amt_replica_mysql_db_port") 
    }

def fetch_mysql_products_data(mysql_config, table_name, fields):
    try:
        # Connect to MySQL
        mysql_conn = mysql.connector.connect(**amt_replica_mysql_db_params)
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        # Build the query
        query = f"SELECT {', '.join(fields)} FROM {table_name}"
        mysql_cursor.execute(query)
        
        # Fetch all rows
        rows = mysql_cursor.fetchall()
        
        # Close the connection
        mysql_cursor.close()
        mysql_conn.close()

        return rows
    except Exception as e:
        print(f"Error fetching data from MySQL: {e}")
        return []


def main():
    # Table and fields
    table_name = "products"
    fields = ["id"]  # Replace with your fields

    # Fetch data from MySQL
    data = fetch_mysql_products_data(amt_replica_mysql_db_params, table_name, fields)
    print(f"Fetched {len(data)} rows from MySQL.")
    print(data)  # Print data for debugging
    
if __name__ == "__main__":
    main()