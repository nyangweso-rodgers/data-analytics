import os
from dotenv import load_dotenv
import clickhouse_connect

# Load environment variables from .env file
#load_dotenv()
load_dotenv(override=True)  # Force reload environment variables


# Retrieve credentials
clickhouse_cloud_host = os.getenv("clickhouse_cloud_host")
clickhouse_cloud_user = os.getenv("clickhouse_cloud_user")
clickhouse_cloud_user_password = os.getenv("clickhouse_cloud_user_password")

# Debug: Print environment variables
print(f"Host: {clickhouse_cloud_host}")
print(f"User: {clickhouse_cloud_user}")
print(f"Password: {clickhouse_cloud_user_password}")

def connect_to_clickhouse_cloud():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_cloud_host,
            user=clickhouse_cloud_user, 
            password=clickhouse_cloud_user_password,
            secure=True # Ensures SSL connection
        )
        print("✅ Connection successful!")
        
        # Test query
        result = client.query("SELECT 1").result_set[0][0]
        print(f"Test Query Result: {result}")
        
        return client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

# Main execution
if __name__ == "__main__":
    # Establish connection
    client = connect_to_clickhouse_cloud()
    
    if client:
        # Perform additional operations here
        print("✅ Connected to ClickHouse Cloud!")
    else:
        print("❌ Failed to connect to ClickHouse Cloud.")