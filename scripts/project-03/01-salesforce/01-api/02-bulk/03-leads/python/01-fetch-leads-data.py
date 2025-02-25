from dotenv import load_dotenv
import os
import requests
import pandas as pd
from salesforce_bulk import SalesforceBulk
import io
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
    domain = instance_url.replace("https://", "").strip("/")
    bulk = SalesforceBulk(sessionId=access_token, host=domain)
    return bulk, access_token, instance_url

def describe_salesforce_object(access_token, instance_url, object_name="Lead"):
    """Retrieve metadata for a Salesforce object to identify compound fields."""
    describe_url = f"{instance_url}/services/data/v58.0/sobjects/{object_name}/describe"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(describe_url, headers=headers)

    compound_fields = set()
    all_fields = set()

    if response.status_code == 200:
        data = response.json()
        print(f"🔍 Checking compound fields in {object_name} object...")
        for field in data["fields"]:
            all_fields.add(field["name"])
            if "compoundFieldName" in field and field["compoundFieldName"]:
                compound_fields.add(field["name"])
            if field["type"] in ["address", "location"]:
                compound_fields.add(field["name"])
            if "fields" in field and isinstance(field["fields"], list):
                compound_fields.add(field["name"])
        print(f"⚠️ Excluding {len(compound_fields)} compound fields: {compound_fields}")
        print("✅ Metadata retrieval complete.")
    else:
        print(f"❌ Failed to describe {object_name}: {response.text}")
    
    return all_fields - compound_fields

def fetch_leads_data(bulk, access_token, instance_url):
    """Fetch leads data using Salesforce Bulk API with filtered fields."""
    valid_fields = describe_salesforce_object(access_token, instance_url, "Lead")
    fields_str = ", ".join(valid_fields)
    query = f"SELECT {fields_str} FROM Lead WHERE LastModifiedDate = THIS_MONTH"

    job = bulk.create_query_job("Lead", contentType="CSV")
    batch = bulk.query(job, query)
    bulk.close_job(job)

    bulk.wait_for_batch(job, batch)
    print("✅ Batch processing completed.")

    results = []
    for result in bulk.get_all_results_for_query_batch(batch):
        for row in result:
            results.append(row.decode("utf-8"))
    
    print(f"📌 Retrieved {len(results)} lead records.")
    # print("🔍 Sample raw CSV rows:", results[:3])  # Show first few rows for inspection
    return results

def validate_leads_data(leads_csv):
    """Validate the leads data, focusing on the 'Id' column."""
    if not leads_csv:
        print("⚠️ No data to validate.")
        return
    
    # Convert CSV rows to DataFrame
    csv_data = io.StringIO("\n".join(leads_csv))
    df = pd.read_csv(csv_data, dtype=str)  # Read as strings to preserve raw data

    # Debugging output
    print(f"📋 DataFrame shape: {df.shape}")
    #print(f"📋 Columns: {list(df.columns)}")
    #print(f"📋 First few rows:\n{df.head(3)}")

    # Check for 'Id' column
    if 'Id' not in df.columns:
        print("❌ 'Id' column not found in data!")
        return
    
    # Analyze 'Id' values
    total_rows = len(df)
    null_ids = df['Id'].isnull().sum()
    empty_ids = (df['Id'] == "").sum()
    valid_ids = total_rows - null_ids - empty_ids

    print(f"🛠 Total rows: {total_rows}")
    print(f"🛠 Rows with NULL 'Id': {null_ids}")
    print(f"🛠 Rows with empty string 'Id': {empty_ids}")
    print(f"🛠 Rows with valid 'Id': {valid_ids}")
    
    if null_ids > 0 or empty_ids > 0:
        print("⚠️ Found rows with missing 'Id' values. Sample of problematic rows:")
        print(df[df['Id'].isnull() | (df['Id'] == "")].head())
    
    # Save to Excel for manual inspection
    output_file = "leads_data_validation.xlsx"
    df.to_excel(output_file, index=False)
    print(f"📥 Saved raw data to {output_file} for inspection.")

def main():
    """Main function to fetch leads and validate the data."""
    start_time = time.time()
    print("🚀 Script execution started...")

    try:
        bulk, access_token, instance_url = get_salesforce_bulk()
        print("✅ Salesforce Bulk API connection established successfully!")
        
        leads_csv = fetch_leads_data(bulk, access_token, instance_url)
        validate_leads_data(leads_csv)
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
    finally:
        end_time = time.time()
        duration_seconds = end_time - start_time
        duration_minutes = duration_seconds / 60
        print(f"⏳ Execution completed in {duration_seconds:.2f} seconds ({duration_minutes:.2f} minutes).")

if __name__ == "__main__":
    main()