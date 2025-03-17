from dotenv import load_dotenv
import os
import requests
import time
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch
import pandas as pd

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

# Database connection parameters
chatbot_db_params = {
    "dbname": os.getenv("chatbot_db"),
    "user": os.getenv("chatbot_db_user"),
    "password": os.getenv("chatbot_db_password"),
    "host": os.getenv("chatbot_db_host"),
    "port": os.getenv("chatbot_db_port")
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

def fetch_salesforce_cds_data(access_token, instance_url):
    """Fetch CDS from Salesforce."""
    headers = {"Authorization": f"Bearer {access_token}"}
    query = """
    SELECT Id,




Clients_Location__Latitude__s,
Clients_Location__Longitude__s,
Clients_Location__c
    FROM Customer_Data_Survey__c
    """
    all_cds = []
    #next_url = f"{instance_url}/services/data/v58.0/query?q={query}"
    next_url = f"{instance_url}/services/data/v58.0/query?q={query}&batchSize=2000"
    
    while next_url:
        response = requests.get(next_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_cds.extend(data.get("records", []))
            next_url = f"{instance_url}{data.get('nextRecordsUrl')}" if "nextRecordsUrl" in data else None
            time.sleep(2)  # Add a 1-second delay
        else:
            print("Failed to fetch CDS:", response.text)
            break
    return all_cds

def save_sf_data_to_excel(cds_data, filename="cds_data_in_excel.xlsx"):
    """Save Salesforce data to an Excel file."""
    if cds_data:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(cds_data)
        
        # Save the DataFrame to an Excel file
        df.to_excel(filename, index=False)
        print(f"Data saved to {filename}")
    else:
        print("No data to save.")
        
def main():
    """Main function to orchestrate the process."""
    # Track start time
    start_time = datetime.now()
    print(f"Script started at: {start_time}")
    
    # Get Salesforce token and fetch CDS
    access_token, instance_url = get_salesforce_token()
    if not access_token:
        print("Authentication failed. Exiting.")
        exit(1)
        
    print("\nFetching CDS from Salesforce...")
    cds = fetch_salesforce_cds_data(access_token, instance_url)
    
    if not cds:
        print("No CDS found or error occurred during fetch.")
        exit(2)
        
    print(f"Found {len(cds)} CDS to process")
    
    # Save CDS data to Excel
    print("\nSaving CDS data to Excel...")
    save_sf_data_to_excel(cds)
    
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