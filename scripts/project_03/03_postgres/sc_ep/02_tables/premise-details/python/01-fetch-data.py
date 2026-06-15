import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DB_NAME = os.getenv("postgres_sunculture_ep_db_name")
DB_USER = os.getenv("postgres_sunculture_ep_db_user")
DB_PASSWORD = os.getenv("postgres_sunculture_ep_db_password")
DB_HOST = os.getenv("postgres_sunculture_ep_db_host")
DB_PORT = os.getenv("postgres_db_port")

def get_database_engine():
    """Creates and returns a SQLAlchemy database engine."""
    try:
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)
        
        # Test connection
        with engine.connect() as connection:
            print("✅ Successfully connected to PostgreSQL!")
        
        return engine
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None  # Return None to indicate failure

def fetch_data(engine, db_table_name, db_table_field_names):
    """Fetch specific fields from the given PostgreSQL table."""
    try:
        # Validate table/field names here or use SQLAlchemy's quoting
        query = text(f"SELECT {db_table_field_names} FROM {db_table_name}")
        print(f"🔄 Executing query: {query}")
        df = pd.read_sql(query, engine)
        print(f"✅ Fetched {len(df)} records from {db_table_name}.")
        return df
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

def save_data_to_excel(df, base_filename, timestamp_format="%Y%m%d_%H%M%S"):
    """Save DataFrame to an Excel file."""
    try:
        # Generate timestamp string
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Split filename and extension
        filename, ext = os.path.splitext(base_filename)
        
        # Create new filename with timestamp
        timestamped_filename = f"{filename}_{timestamp}{ext}"
        
        df.to_excel(timestamped_filename, index=False)
        print(f"✅ Data saved to {timestamped_filename}")
    except Exception as e:
        print(f"❌ Error saving data to Excel: {e}")

def main():
    db_table_name = "premise_details"
    db_table_field_names = "id, premise_id, gps, latitude, longitude, crops_to_be_grown, total_farm_size_acres, county, subcounty"

    # Get database engine
    engine = get_database_engine()
    if engine is None:
        print("❌ Exiting due to database connection failure.")
        return

    # Fetch data
    data = fetch_data(engine, db_table_name, db_table_field_names)

    if data is not None:
        # Save data to Excel with timestamp
        #save_data_to_excel(data, "output-data.xlsx")
        
        # Example: Custom format (date only)
        save_data_to_excel(data, "output_data.xlsx", timestamp_format="%Y-%m-%d")
        
        # Example: Custom format (date and hour-minute)
        #save_data_to_excel(data, "output-data-custom.xlsx", timestamp_format="%Y%m%d_%H%M")

# Execute the main function
if __name__ == "__main__":
    main()