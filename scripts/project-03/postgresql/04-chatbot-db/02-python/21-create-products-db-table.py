import os
import psycopg2

# Database connection parameters
chatbot_postgredb_params = {
    "dbname": os.getenv("chatbot_db"),
    "user": os.getenv("chatbot_db_user"),
    "password": os.getenv("chatbot_db_password"),
    "host": os.getenv("chatbot_db_host"),
    "port": int(os.getenv("chatbot_db_port", 5432))  # Ensure port is an integer
}

# Debugging: Print to check if variables are loaded
# print(chatbot_postgredb_params)

# Function to connect to PostgreSQL
def connect_to_postgres(postgres_config):
    """
    Establish a connection to PostgreSQL.
    
    :param postgres_config: Dictionary containing PostgreSQL connection details.
    :return: PostgreSQL connection object or None if connection fails.
    """
    try:
        postgres_conn = psycopg2.connect(**postgres_config)
        print("✅ Successfully connected to PostgreSQL.")
        return postgres_conn
    except psycopg2.Error as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        return None

# Function to create a table in PostgreSQL
def create_postgres_table(postgres_config, table_name, table_schema):
    """
    Create a table in PostgreSQL.

    :param postgres_config: Dictionary containing PostgreSQL connection details.
    :param table_name: Name of the table to create.
    :param table_schema: SQL statement defining the table schema.
    """
    postgres_conn = connect_to_postgres(postgres_config)
    if not postgres_conn:
        return  # Exit if connection fails
    
    try:
        postgres_cursor = postgres_conn.cursor()

        # Execute the CREATE TABLE statement
        postgres_cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({table_schema})")

        # Commit the transaction
        postgres_conn.commit()
        
        print(f"✅ Table '{table_name}' created successfully in PostgreSQL.")
    
    except psycopg2.Error as e:
        print(f"❌ Error creating table in PostgreSQL: {e}")
        
    finally:
        # Close the connection
        postgres_cursor.close()
        postgres_conn.close()

# Main function
def main():
    # Table name and schema
    table_name = "products"  # Replace with your table name
    table_schema = """
        id SERIAL PRIMARY KEY,
        product VARCHAR(255),
        isActive INTEGER
    """ 

    # Create the table
    create_postgres_table(chatbot_postgredb_params, table_name, table_schema)

if __name__ == "__main__":
    main()
