import os
from dotenv import load_dotenv
import clickhouse_connect

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
clickhouse_cloud_host = os.getenv("clickhouse_cloud_host")
clickhouse_cloud_default_user = os.getenv("clickhouse_cloud_default_user")
#clickhouse_cloud_user = os.getenv("clickhouse_cloud_user")
clickhouse_cloud_password = os.getenv("clickhouse_cloud_password")

# Debug: Print environment variables
print(f"Host: {clickhouse_cloud_host}")
print(f"Default User: {clickhouse_cloud_default_user}")
print(f"Password: {clickhouse_cloud_password}")

def main():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_cloud_host,  # Use the variable, not a string
            user=clickhouse_cloud_default_user,  # Use the variable, not a string
            password=clickhouse_cloud_password,  # Use the variable, not a string
            secure=True
        )
        print("Connection successful!")
        print("Result:", client.query("SELECT 1").result_set[0][0])
    except Exception as e:
        print(f"Error: {e}")
    
if __name__ == '__main__':
    main()