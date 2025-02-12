import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password")

# Salesforce Authentication URL
SF_AUTH_URL = "https://login.salesforce.com/services/oauth2/token"

def get_salesforce_token():
    """Fetch OAuth token from Salesforce."""
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password, 
    }
    response = requests.post(SF_AUTH_URL, data=payload)

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token")
        instance_url = data.get("instance_url")  # Get dynamically
        print("✅ Salesforce Access Token retrieved successfully!")
        return access_token, instance_url  # Return both
    else:
        print("❌ Authentication failed:", response.status_code, response.text)
        return None, None

def get_api_usage(access_token, instance_url):
    """Fetch API usage limits from Salesforce."""
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"{instance_url}/services/data/v58.0/limits", headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print("❌ Failed to fetch API usage:", response.text)
        return None
    
def main():
    """Authenticate and fetch API usage."""
    access_token, instance_url = get_salesforce_token()

    if not access_token or not instance_url:
        print("❌ Authentication failed. Exiting.")
        exit(1)

    # Fetch API usage limits
    api_usage = get_api_usage(access_token, instance_url)

    if api_usage:
        print("✅ API Usage Data:", api_usage)

if __name__ == "__main__":
    main()
