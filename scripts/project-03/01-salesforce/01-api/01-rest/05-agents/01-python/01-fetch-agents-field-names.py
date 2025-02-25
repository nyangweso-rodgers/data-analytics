from dotenv import load_dotenv
import os
import requests
import csv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password")

# Salesforce Authentication URL
sf_instance_url = os.getenv("sf_instance_url")

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
        return data.get("access_token"), data.get("instance_url")  # Return token and instance URL
    else:
        print("Error:", response.status_code, response.text)
        return None, None

def get_agents_fields(access_token, instance_url):
    """Fetch Agents fields from Salesforce, including their data types."""
    headers = {"Authorization": f"Bearer {access_token}"}
    describe_url = f"{instance_url}/services/data/v58.0/sobjects/Agent__c/describe"
    
    response = requests.get(describe_url, headers=headers)
    
    if response.status_code == 200:
        fields_info = [
            (field["name"], field["type"])  # Extract field name and data type
            for field in response.json()["fields"]
        ]
        return fields_info
    else:
        print("Error fetching Agents fields:", response.status_code, response.text)
        return None

def save_to_csv(fields_info, filename="agents_fields_in_csv.csv"):
    """Save Salesforce Agents fields and their data types to a CSV file."""
    if fields_info:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Field Name", "Data Type"])  # Write header row
            writer.writerows(fields_info)  # Write field data
        print(f"Data successfully saved to {filename}")
    else:
        print("No data to save.")

# Run the script
if __name__ == "__main__":
    access_token, instance_url = get_salesforce_token()
    if access_token and instance_url:
        fields_info = get_agents_fields(access_token, instance_url)
        save_to_csv(fields_info)
    else:
        print("Failed to get Salesforce access token.")
