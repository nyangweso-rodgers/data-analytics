import os
import pymysql
from pymysql import Error
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# ============================================================================
# CONFIGURATION
# ============================================================================
DB_CONFIGS = {
    "host": os.getenv("MYSQL_GRDATA_HOST"),
    "port": int(os.getenv("MYSQL_GRDATA_PORT", "3306")),
    "username": os.getenv("MYSQL_GRDATA_USER"),
    "password": os.getenv("MYSQL_GRDATA_PASSWORD", ""),
    "database": os.getenv("MYSQL_GRDATA_DB"),
    "table_name": None,
}

def get_client():
    """
    Creates and returns a MySQL connection object.
    
    Returns:
        pymysql.connections.Connection: Active database connection
        
    Raises:
        Error: If connection fails
    """
    try:
        # First try with SSL (like MySQL Workbench does)
        print("Attempting connection with SSL...")
        try:
            connection = pymysql.connect(
                host=DB_CONFIGS["host"],
                port=DB_CONFIGS["port"],
                user=DB_CONFIGS["username"],
                password=DB_CONFIGS["password"],
                database=DB_CONFIGS["database"],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                ssl={'ssl_mode': 'REQUIRED'}
            )
            print("✓ Connected with SSL")
        except Exception as ssl_error:
            print(f"SSL connection failed: {ssl_error}")
            print("\nAttempting connection without SSL...")
            # Try without SSL
            connection = pymysql.connect(
                host=DB_CONFIGS["host"],
                port=DB_CONFIGS["port"],
                user=DB_CONFIGS["username"],
                password=DB_CONFIGS["password"],
                database=DB_CONFIGS["database"],
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            print("✓ Connected without SSL")
        
        if connection.open:
            print(f"Successfully connected to MySQL database: {DB_CONFIGS['database']}")
            return connection
            
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        print("\nTroubleshooting tips:")
        print("1. Verify your IP is whitelisted in the database server")
        print("2. Confirm the host IP is correct and accessible")
        print("3. Check username, password, and database name")
        print("4. Compare with MySQL Workbench connection settings")
        raise


def establish_db_connection():
    """
    Establishes a database connection and returns both connection and cursor.
    
    Returns:
        tuple: (connection, cursor) objects
        
    Raises:
        Error: If connection or cursor creation fails
    """
    try:
        connection = get_client()
        cursor = connection.cursor()
        
        # Test the connection
        cursor.execute("SELECT DATABASE();")
        db_name = cursor.fetchone()
        print(f"Connected to database: {db_name}")
        
        return connection, cursor
        
    except Error as e:
        print(f"Error establishing database connection: {e}")
        raise


def close_connection(connection, cursor=None):
    """
    Safely closes database connection and cursor.
    
    Args:
        connection: PyMySQL connection object
        cursor: PyMySQL cursor object (optional)
    """
    try:
        if cursor:
            cursor.close()
            print("Cursor closed successfully")
        
        if connection and connection.open:
            connection.close()
            print("Database connection closed successfully")
            
    except Error as e:
        print(f"Error closing connection: {e}")


def main():
    """
    Main function to demonstrate database connection.
    """
    connection = None
    cursor = None
    
    try:
        # Establish connection
        connection, cursor = establish_db_connection()
        
        # Example query - list all tables
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        
        print("\nAvailable tables:")
        for table in tables:
            print(f"  - {list(table.values())[0]}")
        
        # Example query - get server version
        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()
        print(f"\nMySQL Server version: {list(version.values())[0]}")
        
    except Error as e:
        print(f"Database error: {e}")
        
    finally:
        # Always close connection
        close_connection(connection, cursor)


if __name__ == "__main__":
    main()