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

def fetch_leads(sf):
    """
    Fetch leads from Salesforce within a given date range.
    
    :param sf: Salesforce connection object
    :return: List of leads
    """

    # SOQL Query
    query = f"""
        SELECT Id,
IsDeleted,
MasterRecordId,
LastName,
FirstName,
Salutation,
Name,
Title,
Company,
Street,
City,
State,
PostalCode,
Country,
Latitude,
Longitude,
GeocodeAccuracy,
Address,
Phone,
MobilePhone,
Website,
PhotoUrl,
LeadSource,
Status,
Industry,
Rating,
NumberOfEmployees,
OwnerId,
IsConverted,
ConvertedDate,
ConvertedAccountId,
ConvertedContactId,
ConvertedOpportunityId,
IsUnreadByOwner,
CreatedDate,
CreatedById,
LastModifiedDate,
LastModifiedById,
SystemModstamp,
LastActivityDate,
LastViewedDate,
LastReferencedDate,
Jigsaw,
JigsawContactId,
EmailBouncedReason,
EmailBouncedDate,
Acreage__c,
Country_Code__c,
Date_of_Birth__c,
Gender__c,
Lead_AMT_Customer_Id__c,
Installation_Date__c,
Lead_Category__c,
Lead_Channel__c,
Location__c,
Payment_Method__c,
Preferred_Language__c,
Product_del__c,
Purchase_Date__c,
Water_Source_Distance__c,
Water_Source__c,
leadcap__Facebook_Lead_ID__c,
Customer_Type__c,
Lead_Model_Category__c,
ID_Number__c,
Call_Back_Date__c,
Follow_Up_Date__c,
Product__c,
KYC_Status__c,
Agent_Phone_Number__c,
Agent__c,
Payment_Terms__c,
Referral_Name__c,
Referral_ID__c,
Income_Threshold__c,
Last_Updated_By__c,
Daily_Water_Usage__c,
MobileNumberWithCountryCode__c,
Lead_Source_Other_Comment__c,
Total_Dynamic_Head__c,
SmileIdentity_JSON__c,
Referral_Phone_Number__c,
Custom_Opportunity_Name__c,
SMSMessage__c,
Contact_External_Id_Source__c,
ContactRegionId__c,
OpportunityPayPlanId__c,
AMT_Customer_Name__c,
Old_AMT_Customer_ID__c,
Agent_Employee_Number__c,
Other_Phone__c,
Lead_Date_Created__c,
Number_of_Units_Lead__c,
KRA_Pin__c,
Customer_to_Claim_VAT__c,
Customer_Product_of_Interest__c,
My_lead_Filter__c,
Referral_Source_Application__c,
Agent_Referral_SMSBody__c,
Through_Partner_Lead__c,
Unique_Phone_Number__c,
Through_Partner_Customer__c,
Referral_Lead_ID__c,
CDS1Tracker__c,
CDS_Status__c,
Survey_Stat__c,
SADM_Account__c,
SADM_CDS_ID__c,
SADM_Customer__c,
SADM_KYC_Date__c,
SADM_CDS1_Date__c,
SADM_CDS2_Date__c,
SADM_Customer_Creation_Date__c,
SADM_Deposit_Date__c,
SADM_FIRST_MONTH_INSTALLMENT__c,
SADM_JSF_Date__c,
SADM_Status__c,
Employee_ID__c,
Employee_Name__c,
Employee_Phone__c,
Last_Assigned_Agent_Number__c
        FROM Lead
        WHERE LastModifiedDate >= 2025-01-01T00:00:00Z AND LastModifiedDate <= 2025-02-01T23:59:59Z
        LIMIT 10
    """
    
    try:
        result = sf.query(query)  # Execute query
        leads = result.get("records", [])  # Extract records
        return leads
    except Exception as e:
        print(f"Error fetching leads: {e}")
        return []

def save_to_csv(leads, filename="leads_data_in_csv.csv"):
    """Save lead data to a CSV file."""
    if leads:
        # Dynamically get fieldnames from the first lead
        fieldnames = leads[0].keys()  # Extract keys (fields) from the first lead
        
        try:
            with open(filename, mode="w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()  # Write headers (field names)
                for lead in leads:
                    writer.writerow(lead)  # Write the lead data as a row
            print(f"Leads saved to {filename}")
        except Exception as e:
            print(f"Error saving leads to CSV: {e}")
    else:
        print("No leads to save.")

# Example Usage
if __name__ == "__main__":
    # Get Salesforce access token and instance URL
    access_token, instance_url = get_salesforce_token()
    
    if access_token and instance_url:
        # Initialize Salesforce connection
        sf = Salesforce(instance_url=instance_url, session_id=access_token)
        
        # Fetch leads
        leads = fetch_leads(sf)
        
        if leads:
            print(f"Found {len(leads)} leads.")
            save_to_csv(leads)  # Save leads to CSV file
        else:
            print("No leads found in the given date range.")
    else:
        print("Failed to authenticate with Salesforce.")
