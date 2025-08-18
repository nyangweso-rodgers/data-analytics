import clickhouse_connect

# Function to create a user in ClickHouse
def create_user(username, password):
    # Connect to ClickHouse (replace with your credentials)
    client = clickhouse_connect.get_client(
        host="your.clickhouse.cloud.host",
        port=8443,
        username="default",  # Use an admin account
        password="your_admin_password",
        secure=True
    )

    # Create the user
    create_user_query = f"""
    CREATE USER {username} IDENTIFIED WITH plaintext_password BY '{password}'
    """
    client.command(create_user_query)
    print(f"User '{username}' created with password: {password}")

# Example: Create a user with a dynamically generated password
username = "nyangweso_rodgers"
password = generate_password()
create_user(username, password)