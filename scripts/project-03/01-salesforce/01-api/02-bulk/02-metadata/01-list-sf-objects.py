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
    
    # Initialize Salesforce Bulk API
    bulk = SalesforceBulk(sessionId=access_token, host=instance_url.replace("https://", ""))
    
    return bulk, instance_url  # Return both bulk and instance URL
def list_salesforce_objects(bulk, instance_url):  # Now accepts instance_url
    """List available Salesforce objects."""
    try:
        response = requests.get(
            f"{instance_url}/services/data/v58.0/sobjects/",
            headers={"Authorization": f"Bearer {bulk.sessionId}"}
        )
        if response.status_code == 200:
            objects = response.json().get("sobjects", [])
            print("📌 Available Salesforce Objects:")
            for obj in objects[:50]:  # Print only first 5 objects
                print(f"  - {obj['name']}")
        else:
            print("❌ Failed to list objects:", response.text)
    except Exception as e:
        print(f"❌ Error fetching objects: {e}")
def main():
    """Establish Salesforce Bulk API connection and verify objects."""
    bulk, instance_url = get_salesforce_bulk()  # Unpack tuple
    list_salesforce_objects(bulk, instance_url)  # Pass both values

if __name__ == "__main__":
    main()