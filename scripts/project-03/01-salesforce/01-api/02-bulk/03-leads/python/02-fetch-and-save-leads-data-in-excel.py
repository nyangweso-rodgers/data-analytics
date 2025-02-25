from dotenv import load_dotenv
import os
import requests
import pandas as pd
from salesforce_bulk import SalesforceBulk
import io
import csv
import time

# Load environment variables
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
    all_fields = set()  # Store all available fields

    if response.status_code == 200:
        data = response.json()
        print(f"🔍 Checking compound fields in {object_name} object...")

        for field in data["fields"]:
            all_fields.add(field["name"])  # Track all field names

            # Check if explicitly marked as compound
            if "compoundFieldName" in field and field["compoundFieldName"]:
                compound_fields.add(field["name"])

            # Check known compound types (Name, Address, Geolocation)
            if field["type"] in ["address", "location"]:
                compound_fields.add(field["name"])

            # Check if the field contains subfields (nested structure)
            if "fields" in field and isinstance(field["fields"], list):
                compound_fields.add(field["name"])

        print(f"⚠️ Excluding {len(compound_fields)} compound fields: {compound_fields}")
        print("✅ Metadata retrieval complete.")
    else:
        print(f"❌ Failed to describe {object_name}: {response.text}")

    # Return all fields minus compound fields
    return all_fields - compound_fields


def fetch_leads_data(bulk, access_token, instance_url):
    """Fetch leads data using Salesforce Bulk API with filtered fields."""
    
    # Get valid fields excluding compound fields
    valid_fields = describe_salesforce_object(access_token, instance_url, "Lead")

    # Construct dynamic SELECT query
    fields_str = ", ".join(valid_fields)
    query = f"SELECT {fields_str} FROM Lead WHERE LastModifiedDate = THIS_MONTH"

    job = bulk.create_query_job("Lead", contentType="CSV")
    batch = bulk.query(job, query)
    bulk.close_job(job)

    # Wait for batch to complete
    bulk.wait_for_batch(job, batch)
    print("✅ Batch processing completed.")

    # Retrieve results
    results = []
    for result in bulk.get_all_results_for_query_batch(batch):
        for row in result:
            results.append(row.decode("utf-8"))  # Decode bytes to strings

    return results


def parse_csv_from_bytes(csv_rows):
    """Convert CSV rows into a Pandas DataFrame."""
    if not csv_rows:
        print("⚠️ No data to process.")
        return None

    # Read CSV using StringIO
    csv_data = io.StringIO("\n".join(csv_rows))
    reader = csv.reader(csv_data)

    # Convert to DataFrame
    data = list(reader)
    df = pd.DataFrame(data[1:], columns=data[0])  # First row as headers
    return df


def save_dataframe_to_excel(df, filename="salesforce_leads.xlsx"):
    """Save DataFrame to an Excel file."""
    if df is None or df.empty:
        print("⚠️ No data to save.")
        return

    df.to_excel(filename, index=False)
    print(f"✅ Leads data successfully saved to {filename}")


def main():
    """Main function to fetch leads and save them to an Excel file."""
    start_time = time.time()  # Track start time
    print("🚀 Script execution started...")

    try:
        # Initialize Bulk API connection
        bulk, access_token, instance_url = get_salesforce_bulk()
        print("✅ Salesforce Bulk API connection established successfully!")

        # Fetch raw CSV data
        leads_csv = fetch_leads_data(bulk, access_token, instance_url)
        print(f"📌 Retrieved {len(leads_csv)} lead records.")

        # Convert to DataFrame
        df = parse_csv_from_bytes(leads_csv)

        # Save to Excel
        if df is not None:
            save_dataframe_to_excel(df)

    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        # Track execution time
        end_time = time.time()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60

        print(f"⏳ Execution completed in {duration_seconds:.2f} seconds ({duration_minutes:.2f} minutes).")


if __name__ == "__main__":
    main()
