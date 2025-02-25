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
    bulk = SalesforceBulk(sessionId=access_token, host=domain)

    return bulk, access_token, instance_url


def describe_salesforce_object(access_token, instance_url, object_name="Lead"):
    """Retrieve metadata for a Salesforce object to identify compound fields."""
    describe_url = f"{instance_url}/services/data/v58.0/sobjects/{object_name}/describe"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(describe_url, headers=headers)

    compound_fields = set()  # Store compound fields

    if response.status_code == 200:
        data = response.json()
        print(f"🔍 Checking compound fields in {object_name} object...")

        for field in data["fields"]:
            # Check if explicitly marked as compound
            if "compoundFieldName" in field and field["compoundFieldName"]:
                compound_fields.add(field["name"])
                print(f"❌ {field['name']} is a compound field! Avoid using it.")
            
            # Check known compound types (Name, Address, Geolocation)
            if field["type"] in ["address", "location"]:
                compound_fields.add(field["name"])
                print(f"❌ {field['name']} is a compound field! Avoid using it.")

            # Check if the field contains subfields (nested structure)
            if "fields" in field and isinstance(field["fields"], list):
                compound_fields.add(field["name"])
                print(f"❌ {field['name']} is a compound field with subfields! Avoid using it.")

        print("✅ Metadata retrieval complete.")
    else:
        print(f"❌ Failed to describe {object_name}: {response.text}")

    return compound_fields  # Return compound fields for filtering


def main():
    """Establish Salesforce Bulk API connection and describe object fields."""
    bulk, access_token, instance_url = get_salesforce_bulk()
    print("✅ Salesforce Bulk API connection established successfully!")

    # Fetch and check compound fields
    describe_salesforce_object(access_token, instance_url, object_name="Lead")


if __name__ == "__main__":
    main()
