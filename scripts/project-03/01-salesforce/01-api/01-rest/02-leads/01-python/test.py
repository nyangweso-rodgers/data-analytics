from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
import traceback

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password")

# print("Client ID:", sf_client_id)  # Debugging: Check if value is loaded

# Salesforce Authentication URL
sf_instance_url = os.getenv("sf_instance_url")

# Global variable to store access token and expiry time
access_token = None
token_expiry = None

# PostgreSQL configuration
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

def get_salesforce_token():
    """Fetch OAuth token from Salesforce."""
    sf_auth_url="https://login.salesforce.com/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password, 
    }
    response = requests.post(sf_auth_url, data=payload)
    #return response.json()["access_token"]
    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        instance_url = data.get("instance_url")  # Get dynamically
        print("Salesforce Access Token:", access_token)
        return access_token, instance_url  # Return both
    else:
        print("Error:", response.status_code, response.text)
        return None
    
def fetch_salesforce_leads(access_token, instance_url):
    """Fetch Leads from Salesforce."""
    headers = {"Authorization": f"Bearer {access_token}"}
    query = """
        SELECT Id,
 


        FROM Lead
        WHERE LastModifiedDate = THIS_MONTH
    """
    all_leads = []
    next_url = f"{instance_url}/services/data/v58.0/query?q={query}"
    
    while next_url:
        response = requests.get(next_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_leads.extend(data.get("records", []))
            next_url = f"{instance_url}{data.get('nextRecordsUrl')}" if "nextRecordsUrl" in data else None
        else:
            print("Failed to fetch Leads:", response.text)
            break
    return all_leads

    
def save_to_postgresql(leads):
    """Save Leads to PostgreSQL."""
    db_connection = psycopg2.connect(**ep_stage_db_params)
    cursor = db_connection.cursor()
    
    # Upsert query (adjust fields based on your schema)
    query = """
    INSERT INTO sf_leads (
        Id,

    )
    VALUES (
        %(Id)s,

    )
    ON CONFLICT (Id) 
    DO UPDATE SET 
    IsDeleted = EXCLUDED.IsDeleted,

    """
    try:
        # Prepare data
        data = []
        for lead in leads:
            # Check each field for length violations
            lead_data = {
                # Map all fields here
                'Id': lead.get('Id'),
                # other fields names goes here
            }
            # Validate and truncate fields if necessary
            for field_name, value in lead_data.items():
                if isinstance(value, str):
                    # Check if the value exceeds the PostgreSQL column length
                    if len(value) > 18:  # Adjust the length limit as needed
                        #print(f"Warning: Field '{field_name}' exceeds length limit. Value: '{value}'")
                        lead_data[field_name] = value[:18]  # Truncate to 18 characters
            data.append(lead_data)
        
        # Attempt to insert leads in batches
        execute_batch(cursor, query, data)
        db_connection.commit()
        print(f"Successfully upserted {len(leads)} leads")
    except Exception as e:
        db_connection.rollback()
        # Log the full exception for debugging
        print(f"Error saving leads: {str(e)}")
        print("Traceback:", traceback.format_exc())  # Log full traceback
        
    finally:
        cursor.close()
        db_connection.close()

def main():
    """Main function to orchestrate the process."""
    # Track start time
    start_time = datetime.now()
    print(f"Script started at: {start_time}")
    
    # Get Salesforce token and fetch leads (as before)
    access_token, instance_url = get_salesforce_token()

    if not access_token:
        print("Authentication failed. Exiting.")
        exit(1)
        
    print("\nFetching leads from Salesforce...")
    leads = fetch_salesforce_leads(access_token, instance_url)
    
    if not leads:
        print("No leads found or error occurred during fetch.")
        exit(2)
        
    print(f"Found {len(leads)} leads to process")
    
    # Save leads to PostgreSQL
    print("\nSaving leads to PostgreSQL...")
    save_to_postgresql(leads)
    print("Process completed successfully!")
    
    # Track end time and calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"Script ended at: {end_time}")
    print(f"Total duration: {duration}")

    # Check if the duration exceeds 15 minutes (AWS Lambda limit)
    if duration > timedelta(minutes=15):
        print("Warning: Script runtime exceeds 15 minutes!")
    else:
        print("Script completed within the 15-minute runtime limit.")

if __name__ == "__main__":
    main()