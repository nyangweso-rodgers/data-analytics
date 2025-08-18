import os
from dotenv import load_dotenv
import clickhouse_connect
import secrets
import string

# Load environment variables from .env file
#load_dotenv()
load_dotenv(override=True)  # Force reload environment variables

# Retrieve credentials
clickhouse_cloud_host = os.getenv("clickhouse_cloud_host")
clickhouse_cloud_default_user = os.getenv("clickhouse_cloud_default_user")
clickhouse_cloud_password = os.getenv("clickhouse_cloud_password")

# Debug
print(f"Host: {clickhouse_cloud_host}")
print(f"User: {clickhouse_cloud_default_user}")
print(f"Password: {clickhouse_cloud_password}")

# Establish connection to ClickHouse Cloud
def connect_to_clickhouse_cloud():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_cloud_host,
            user=clickhouse_cloud_default_user, 
            password=clickhouse_cloud_password,
            secure=True
        )
        print("✅ Connection successful!")
        
        # Test query
        result = client.query("SELECT 1").result_set[0][0]
        print(f"Test Query Result: {result}")
        
        return client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

# Function to generate a random password
def generate_password(length=12):
    # Define character sets
    lowercase = string.ascii_lowercase
    uppercase = string.ascii_uppercase
    digits = string.digits
    special = string.punctuation

    # Ensure the password contains at least one character from each set
    password = [
        secrets.choice(lowercase),
        secrets.choice(uppercase),
        secrets.choice(digits),
        secrets.choice(special),
    ]

    # Fill the rest of the password with random characters
    remaining_length = length - len(password)
    all_characters = lowercase + uppercase + digits + special
    password.extend(secrets.choice(all_characters) for _ in range(remaining_length))

    # Shuffle the password to avoid predictable patterns
    secrets.SystemRandom().shuffle(password)

    return ''.join(password)

# Function to create a user in ClickHouse
def create_user(client, username, password):
    try:
        create_user_query = f"""
        CREATE USER {username} IDENTIFIED WITH plaintext_password BY '{password}'
        """
        client.command(create_user_query)
        print(f"✅ User '{username}' created with password: {password}")
    except Exception as e:
        print(f"❌ Failed to create user '{username}': {e}")

# Function to grant permissions
def grant_permissions(client, username, database, table, permission="ALL"):
    try:
        grant_query = f"""
        GRANT {permission} ON {database}.{table} TO {username}
        """
        client.command(grant_query)
        print(f"✅ Granted {permission} on {database}.{table} to {username}")
    except Exception as e:
        print(f"❌ Failed to grant permissions to '{username}': {e}")

# Example usage
if __name__ == "__main__":
    # Establish connection
    client = connect_to_clickhouse_cloud()
    
    if client:
        # Create a new user
        username = "nyangweso_rodgers"
        password = generate_password()
        create_user(client, username, password)
        
        # Grant permissions
        grant_permissions(client, username, "default", "ALL")
        
        # Close the connection when done
        client.close()
        print("✅ Connection closed.")