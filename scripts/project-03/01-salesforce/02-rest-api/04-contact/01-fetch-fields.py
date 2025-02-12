from dotenv import load_dotenv
import os
import json
import requests


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

def get_contact_fields(access_token, instance_url):
    """Fetch contact from Salesforce."""
    headers = {"Authorization": f"Bearer {access_token}"}
    describe_url = f"{instance_url}/services/data/v58.0/sobjects/Contact/describe"
    response = requests.get(describe_url, headers=headers)
    
    if response.status_code == 200:
        fields = [field["name"] for field in response.json()["fields"]]
        return fields
    else:
        print("Error fetching contact fields:", response.status_code, response.text)
        return None
    
# Run the test
if __name__ == "__main__":
    access_token, instance_url = get_salesforce_token()
    if access_token and instance_url:
        fields = get_contact_fields(access_token, instance_url)
        print("Fields in Contact:", fields)
    else:
        print("Failed to get Salesforce access token.")