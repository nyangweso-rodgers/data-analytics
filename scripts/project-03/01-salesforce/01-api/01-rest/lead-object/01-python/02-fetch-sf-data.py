from dotenv import load_dotenv
import os
import requests
import csv
import pandas as pd
from simple_salesforce import Salesforce
from datetime import datetime, timedelta

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
        #print("Salesforce Access Token:", access_token)
        print("Successfully established connection to Salesforce.")  # Generic success message
        return access_token, instance_url  # Return both
    else:
        print("Error:", response.status_code, response.text)
        return None, None
    
# Define fields for each Salesforce object
sf_object_fields = {
    "Lead": [
        "Id", 
        "IsDeleted", "LastName", "FirstName", "Name", 
        "LeadSource", "Status",
        "OwnerId", "IsConverted", "ConvertedDate", "ConvertedAccountId", "ConvertedContactId",
        "ConvertedOpportunityId", 
        "CreatedDate", "Lead_Date_Created__c", "SADM_Customer_Creation_Date__c","SADM_Deposit_Date__c",
        "LastModifiedDate", "SystemModstamp",
        "CreatedById",
        "LastModifiedById", "Last_Updated_By__c",
        "SADM_Customer__c",
        "ID_Number__c","Phone", "MobileNumberWithCountryCode__c", "Unique_Phone_Number__c", "Other_Phone__c",
        "Country_Code__c", "Lead_Category__c", "Lead_Channel__c", "Location__c", 
        "Product__c", 
        "Agent__c", "Agent_Phone_Number__c", "Agent_Employee_Number__c",
        "Referral_Name__c", "Referral_ID__c", 
        "Referral_Phone_Number__c", 
        "Lead_AMT_Customer_Id__c",
        "OpportunityPayPlanId__c", 
        "AMT_Customer_Name__c", 
        "Customer_Product_of_Interest__c",
        "Referral_Source_Application__c", 
        "Employee_ID__c", "Employee_Name__c", "Employee_Phone__c",
        "SADM_JSF_Date__c", "Purchase_Date__c"
    ]
}

def fetch_sf_records(sf, object_name, start_date, end_date):
    """
    Fetch records from Salesforce within a given date range.
    
    :param sf: Salesforce connection object
    :param object_name: Name of the Salesforce object (e.g., 'Lead')
    :param start_date: Start date in ISO format (e.g., '2025-01-01T00:00:00Z')
    :param end_date: End date in ISO format (e.g., '2025-03-12T23:59:59Z')
    :param limit: Maximum number of records to retrieve (default: 10)
    :return: List of records
    """
    # Get fields for the specified object
    fields = sf_object_fields.get(object_name)
    if not fields:
        print(f"No fields defined for object: {object_name}")
        return []
    
    # Construct SOQL Query
    query = f"""
        SELECT {', '.join(fields)}
        FROM {object_name}
        WHERE CreatedDate >= {start_date} AND CreatedDate <= {end_date}
    """

    try:
        all_records = []
        result = sf.query(query)  # Execute query
        records = result.get("records", []) # Extract records
        
        # Remove the 'attributes' field from each record
        for record in records:
            record.pop("attributes", None)
            
        all_records.extend(records)  # Add records to the list
        
        # Handle pagination if there are more records
        while not result['done']:
            next_records_url = result["nextRecordsUrl"]
            result = sf.query_more(next_records_url, identifier_is_url=True)
            records = result.get("records", [])
            
            # Remove the 'attributes' field from each record
            for record in records:
                record.pop("attributes", None)
            
            all_records.extend(records)  # Add records to the list
            
        print(f"Successfully fetched {len(all_records)} records from {object_name}.")
        return all_records
    except Exception as e:
        print(f"Error fetching records from {object_name}: {e}")
        return []

def save_sf_data_to_csv(records, filename="salesforce-data.csv"):
    """Save Salesforce data to a CSV file."""
    if records:
        # Dynamically get fieldnames from the first record
        fieldnames = records[0].keys()  # Extract keys (fields) from the first record
        
        try:
            with open(filename, mode="w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()  # Write headers (field names)
                for record in records:
                    writer.writerow(record)  # Write the record data as a row
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving data to CSV: {e}")
    else:
        print("No records to save.")

def save_sf_data_to_excel(records, filename="salesforce-data.xlsx"):
    """Save Salesforce data to an Excel file."""
    if records:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(records)
        
        # Save the DataFrame to an Excel file
        df.to_excel(filename, index=False)
        print(f"Data saved to {filename}")
    else:
        print("No data to save.")

def main():
    """Main function to execute the script."""
    # Track start time
    start_time = datetime.now()
    print(f"Script started at: {start_time}")
    
    # Get Salesforce access token and instance URL
    access_token, instance_url = get_salesforce_token()
    
    if access_token and instance_url:
        # Initialize Salesforce connection
        sf = Salesforce(instance_url=instance_url, session_id=access_token)
        
        # Define object name and date range
        object_name = "Lead"  # Change this to the desired object (e.g., "Account", "Contact")
        start_date = "2025-01-01T00:00:00Z"
        end_date = "2025-03-12T23:59:59Z"
        
        # Fetch records
        records = fetch_sf_records(sf, object_name, start_date, end_date)
        
        if records:
            print(f"Found {len(records)} records from {object_name}.")
            # Save records to CSV or Excel
            #save_sf_data_to_csv(records, filename=f"{object_name.lower()}-data-csv.csv")
            save_sf_data_to_excel(records, filename=f"{object_name.lower()}-data-excel.xlsx")
        
        else:
            print(f"No records found in {object_name} for the given date range.")
    else:
        print("Failed to authenticate with Salesforce.")
    # Record script end time
    end_time = datetime.now()
    print(f"Script ended at: {end_time}")

    # Calculate and print script duration
    duration = end_time - start_time
    print(f"Script duration: {duration}")

# Execute the main function
if __name__ == "__main__":
    main()
