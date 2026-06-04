import os 
import mysql.connector
import psycopg2

# Database connection parameters
amt_replica_mysql_db_params = {
    "database": os.getenv("amt_replica_mysql_db"),
    "user": os.getenv("amt_replica_mysql_db_user"),
    "password": os.getenv("amt_replica_mysql_db_password"),
    "host": os.getenv("amt_replica_mysql_db_host"),
    "port": os.getenv("amt_replica_mysql_db_port") 
    }

chatbot_postgredb_params = {
    "dbname": os.getenv("chatbot_db"),
    "user": os.getenv("chatbot_db_user"),
    "password": os.getenv("chatbot_db_password"),
    "host": os.getenv("chatbot_db_host"),
    "port": os.getenv("chatbot_db_port")
}

# Debug: Print connection parameters
print("MySQL Connection Params:", amt_replica_mysql_db_params)
print("PostgreSQL Connection Params:", chatbot_postgredb_params)

# Function to fetch specific fields from MySQL
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

# Function to sync data to PostgreSQL
def sync_products_data_to_postgresdb(postgres_config, table_name, fields, data):
    try:
        # Connect to PostgreSQL
        postgres_conn = psycopg2.connect(**chatbot_postgredb_params)
        postgres_cursor = postgres_conn.cursor()
        
        # Build the query
        columns = ', '.join(fields)
        placeholders = ', '.join(['%s'] * len(fields))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Insert each row
        for row in data:
            values = [row[field] for field in fields]
            postgres_cursor.execute(query, values)
        
        # Commit the transaction
        postgres_conn.commit()
        
         # Close the connection
        postgres_cursor.close()
        postgres_conn.close()

        print(f"Synced {len(data)} rows to PostgreSQL.")
    except Exception as e:
        print(f"Error syncing data to PostgreSQL: {e}")

def main():
    # Table and fields
    amt_mysql_table_name = "products"  # MySQL table name
    postgres_table_name = "products"   # PostgreSQL table name
    fields = ["id", "product", "isactive"]  # Replace with your fields

   # Fetch data from MySQL
    data = fetch_mysql_products_data(amt_replica_mysql_db_params, amt_mysql_table_name, fields)
    print(f"Fetched {len(data)} rows from MySQL.")
    print(data)  # Print data for debugging
    
    # Sync data to PostgreSQL
    if data:
        sync_products_data_to_postgresdb(chatbot_postgredb_params, postgres_table_name, fields, data)
    
if __name__ == "__main__":
    main()