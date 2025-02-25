from dotenv import load_dotenv
import os 
import requests
from salesforce_bulk import SalesforceBulk

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password") + os.getenv("sf_security_token", "")

# Salesforce Authentication URL
SF_AUTH_URL = "https://login.salesforce.com/services/oauth2/token"


def get_salesforce_token():
    """Fetch OAuth token from Salesforce."""
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password
    }
    response = requests.post(SF_AUTH_URL, data=payload)
    
    if response.status_code == 200:
        data = response.json()
        access_token = data["access_token"]
        instance_url = data["instance_url"].strip("/")
        print("Salesforce Access Token:", access_token)
        print("Instance URL:", instance_url)
        print("✅ Successfully authenticated with Salesforce!")
        return access_token, instance_url
    else:
        print(f"❌ Authentication failed: {response.text}")
        exit(1)
def get_salesforce_bulk():
    """Initialize Salesforce Bulk API connection using OAuth token."""
    access_token, instance_url = get_salesforce_token()
    
    # Extract domain from instance URL (remove "https://")
    domain = instance_url.replace("https://", "").strip("/")
    
    # Initialize Salesforce Bulk API
    return SalesforceBulk(
        sessionId=access_token,
        host=domain
    )

def main():
    """Establish Salesforce Bulk API connection."""
    bulk = get_salesforce_bulk()
    print("✅ Salesforce Bulk API connection established successfully!")

if __name__ == "__main__":
    main()