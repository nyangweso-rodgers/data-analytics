import os
from dotenv import load_dotenv
import requests
import psycopg2
import re

# Load environment variables
load_dotenv()

# KoboToolbox API details
KOBO_API_URL = "https://kf.kobotoolbox.org/api/v2/assets/aNo2GBnQSU8rghPQGmqTt9/data/?format=json"
KOBO_API_TOKEN = os.getenv("KOBO_API_TOKEN")
HEADERS = {
    "Authorization": f"Token {KOBO_API_TOKEN}",
    "Accept": "application/json"
}

# Database connection parameters
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

# Specify the fields to include in the extracted data
INCLUDED_FIELDS = {
    "date", "Name_of_surveyor", "_uuid"  # Expanded for debugging; adjust as needed
}

def connect_to_kobotoolbox():
    """Establish connection to KoboToolbox API."""
    response = requests.get(KOBO_API_URL, headers=HEADERS)
    return response

def fetch_kobo_data():
    """Fetch KoboToolbox data with pagination and return only specified fields."""
    all_data = []
    url = KOBO_API_URL
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"❌ Error: {response.status_code}, {response.text}")
            return None
        
        try:
            data = response.json()
            if "results" in data:
                filtered_data = [
                    {key: value for key, value in entry.items() if key in INCLUDED_FIELDS}
                    for entry in data["results"]
                ]
                all_data.extend(filtered_data)
                url = data.get("next")  # Follow pagination
                print(f"📥 Fetched {len(data['results'])} records, total so far: {len(all_data)}")
            else:
                print("❌ No results found in response.")
                return None
        except requests.exceptions.JSONDecodeError as e:
            print(f"❌ JSON Decode Error: {e}")
            return None
    
    return all_data

def connect_to_postgres():
    """Establishes a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(**ep_stage_db_params)
        print("✅ Connected to PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return None
    
def sanitize_column_name(field_name):
    """Replace special characters in field names to make them PostgreSQL-compatible."""
    return field_name.lower()

def save_to_postgres(data, table_name="carbon_2023_vintage"):
    """Save filtered KoboToolbox data to PostgreSQL."""
    conn = connect_to_postgres()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # Convert field names to lowercase
        sanitized_fields = [sanitize_column_name(field) for field in INCLUDED_FIELDS]
        columns = ", ".join(sanitized_fields)
        values_placeholders = ", ".join(["%s"] * len(sanitized_fields))
        insert_query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({values_placeholders})
        ON CONFLICT (_uuid) DO NOTHING;
        """

        total_inserted = 0
        for entry in data:
            try:
                record = tuple(
                    None if isinstance(entry.get(field), (dict, list)) else entry.get(field, None)
                    for field in INCLUDED_FIELDS
                )
                cursor.execute(insert_query, record)
                if cursor.rowcount > 0:  # Only count if a row was actually inserted
                    total_inserted += cursor.rowcount
                else:
                    print(f"ℹ️ Skipped record with _uuid: {entry.get('_uuid')} (duplicate)")
            except Exception as e:
                error_uuid = entry.get("_uuid", "Unknown UUID")
                print(f"❌ Error inserting record with _uuid: {error_uuid} - {str(e)}")
                print(f"Record data: {record}")
                raise

        conn.commit()
        print(f"✅ Successfully inserted {total_inserted} records out of {len(data)} fetched.")
    except Exception as e:
        print(f"❌ Error saving to PostgreSQL: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main execution function."""
    data = fetch_kobo_data()
    if data:
        print(f"✅ Retrieved {len(data)} records from KoboToolbox.")
        save_to_postgres(data)

if __name__ == "__main__":
    main()