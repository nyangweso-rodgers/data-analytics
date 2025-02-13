from dotenv import load_dotenv
import os
import requests
import csv
from simple_salesforce import Salesforce

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password")

def get_salesforce_token():
    """Fetch OAuth token from Salesforce."""
    sf_auth_url = "https://login.salesforce.com/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password,
    }
    response = requests.post(sf_auth_url, data=payload)
    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        instance_url = data.get("instance_url")  # Get dynamically
        print("Salesforce Access Token:", access_token)
        return access_token, instance_url  # Return both
    else:
        print("Error:", response.status_code, response.text)
        return None, None
    
def fetch_cds_data(sf):
    # SOQL Query
    query = f"""
    select COUNT() 
    from Customer_Data_Survey__c
    """
    
    try:
        query_results = sf.query(query) # Execute Query
        total_count = query_results.get("totalSize", 0)  # Extract total count
        return total_count
    except Exception as e:
        print(f"Error fetching CDS Data: {e}")
        return 0
    
if __name__ == "__main__":
    # Get Salesforce access token and instance URL
    access_token, instance_url = get_salesforce_token()
    
    if access_token and instance_url:
        # Initialize Salesforce connection
        sf = Salesforce(instance_url=instance_url, session_id=access_token)
        
        # Fetch CDS count
        cds_count = fetch_cds_data(sf)
        
        if cds_count > 0:
            print(f"Found {cds_count} CDS records.")
        else:
            print("No CDS records found.")
    else:
        print("Failed to authenticate with Salesforce.")