from dotenv import load_dotenv
import os
import json
import requests
import psycopg2
from psycopg2.extras import execute_batch

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

# PostgreSQL configuration
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

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
    
def fetch_salesforce_leads(access_token, instance_url):
    """Fetch Leads from Salesforce."""
    headers = {"Authorization": f"Bearer {access_token}"}
    query = """
        SELECT Id,
        CreatedDate,
        LastModifiedDate,
        LastActivityDate,
        Lead_Date_Created__c,
        SystemModstamp,
        CreatedById,
        LastModifiedById,
        Last_Updated_By__c,
        FirstName,
        LastName,
        Name,
        Company,
        Country,
        Country_Code__c,
        Location__c,
        Gender__c,
        Date_of_Birth__c,
        Referral_ID__c,
        Referral_Name__c,
        Referral_Phone_Number__c,
        Lead_Category__c,
        Lead_Channel__c,
        Agent__c,
        Agent_Referral_SMSBody__c,
        Customer_Type__c,
        IsDeleted,
        Phone,
        MobilePhone,
        Lead_AMT_Customer_Id__c,
        Installation_Date__c,
        LastViewedDate,
        LastReferencedDate,
        Payment_Method__c,
        Lead_Model_Category__c,
        ID_Number__c,
        Call_Back_Date__c,
        Follow_Up_Date__c,
        Product__c,
        KYC_Status__c,
        Agent_Phone_Number__c,
        Preferred_Language__c,
        Product_del__c,
        Purchase_Date__c,
        Water_Source_Distance__c,
        Water_Source__c,
        leadcap__Facebook_Lead_ID__c,
        Payment_Terms__c,
        Acreage__c,
        Employee_ID__c,
        Employee_Name__c,
        Employee_Phone__c,
        Last_Assigned_Agent_Number__c,
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
        Number_of_Units_Lead__c,
        KRA_Pin__c,
        Customer_to_Claim_VAT__c,
        Customer_Product_of_Interest__c,
        My_lead_Filter__c,
        Referral_Source_Application__c,
        Daily_Water_Usage__c,
        MobileNumberWithCountryCode__c,
        Lead_Source_Other_Comment__c,
        Total_Dynamic_Head__c,
        Custom_Opportunity_Name__c,
        SMSMessage__c,
        Contact_External_Id_Source__c,
        ContactRegionId__c,
        OpportunityPayPlanId__c,
        AMT_Customer_Name__c,
        Old_AMT_Customer_ID__c,
        Agent_Employee_Number__c,
        Other_Phone__c,
        Income_Threshold__c,
        Street,
        City,
        State,
        PostalCode,
        Title,
        Latitude,
        Longitude,
        GeocodeAccuracy,
        Address,
        MasterRecordId
        FROM Lead
    """
    all_leads = []
    next_url = f"{instance_url}/services/data/v58.0/query?q={query}"
    
    while next_url:
        response = requests.get(next_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            all_leads.extend(data.get("records", []))
            next_url = f"{instance_url}{data.get('nextRecordsUrl')}" if "nextRecordsUrl" in data else None
        else:
            print("Failed to fetch Leads:", response.text)
            break
    return all_leads

def save_to_postgresql(leads):
    """Save Leads to PostgreSQL."""
    db_connection = psycopg2.connect(**ep_stage_db_params)
    cursor = db_connection.cursor()
    
    # Upsert query (adjust fields based on your schema)
    query = """
    INSERT INTO sf_leads (
        Id,
        CreatedDate,
        LastModifiedDate,
        LastActivityDate,
        Lead_Date_Created__c,
        SystemModstamp,
        CreatedById,
        LastModifiedById,
        Last_Updated_By__c,
        FirstName,
        LastName,
        Name,
        Company,
        Country,
        Country_Code__c,
        Location__c,
        Gender__c,
        Date_of_Birth__c,
        Referral_ID__c,
        Referral_Name__c,
        Referral_Phone_Number__c,
        Lead_Category__c,
        Lead_Channel__c,
        Agent__c,
        Agent_Referral_SMSBody__c,
        Customer_Type__c,
        IsDeleted,
        Phone,
        MobilePhone,
        Lead_AMT_Customer_Id__c,
        Installation_Date__c,
        LastViewedDate,
        LastReferencedDate,
        Payment_Method__c,
        Lead_Model_Category__c,
        ID_Number__c,
        Call_Back_Date__c,
        Follow_Up_Date__c,
        Product__c,
        KYC_Status__c,
        Agent_Phone_Number__c,
        Preferred_Language__c,
        Product_del__c,
        Purchase_Date__c,
        Water_Source_Distance__c,
        Water_Source__c,
        leadcap__Facebook_Lead_ID__c,
        Payment_Terms__c,
        Acreage__c,
        Employee_ID__c,
        Employee_Name__c,
        Employee_Phone__c,
        Last_Assigned_Agent_Number__c,
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
        Number_of_Units_Lead__c,
        KRA_Pin__c,
        Customer_to_Claim_VAT__c,
        Customer_Product_of_Interest__c,
        My_lead_Filter__c,
        Referral_Source_Application__c,
        Daily_Water_Usage__c,
        MobileNumberWithCountryCode__c,
        Lead_Source_Other_Comment__c,
        Total_Dynamic_Head__c,
        Custom_Opportunity_Name__c,
        SMSMessage__c,
        Contact_External_Id_Source__c,
        ContactRegionId__c,
        OpportunityPayPlanId__c,
        AMT_Customer_Name__c,
        Old_AMT_Customer_ID__c,
        Agent_Employee_Number__c,
        Other_Phone__c,
        Income_Threshold__c,
        Street,
        City,
        State,
        PostalCode,
        Title,
        Latitude,
        Longitude,
        GeocodeAccuracy,
        Address,
        MasterRecordId
    )
    VALUES (
        %(Id)s,
        %(CreatedDate)s,
        %(LastModifiedDate)s,
        %(LastActivityDate)s,
        %(Lead_Date_Created__c)s,
        %(SystemModstamp)s,
        %(CreatedById)s,
        %(LastModifiedById)s,
        %(Last_Updated_By__c)s,
        %(FirstName)s,
        %(LastName)s,
        %(Name)s,
        %(Company)s,
        %(Country)s,
        %(Country_Code__c)s,
        %(Location__c)s,
        %(Gender__c)s,
        %(Date_of_Birth__c)s,
        %(Referral_ID__c)s,
        %(Referral_Name__c)s,
        %(Referral_Phone_Number__c)s,
        %(Lead_Category__c)s,
        %(Lead_Channel__c)s,
        %(Agent__c)s,
        %(Agent_Referral_SMSBody__c)s,
        %(Customer_Type__c)s,
        %(IsDeleted)s,
        %(Phone)s,
        %(MobilePhone)s,
        %(Lead_AMT_Customer_Id__c)s,
        %(Installation_Date__c)s,
        %(LastViewedDate)s,
        %(LastReferencedDate)s,
        %(Payment_Method__c)s,
        %(Lead_Model_Category__c)s,
        %(ID_Number__c)s,
        %(Call_Back_Date__c)s,
        %(Follow_Up_Date__c)s,
        %(Product__c)s,
        %(KYC_Status__c)s,
        %(Agent_Phone_Number__c)s,
        %(Preferred_Language__c)s,
        %(Product_del__c)s,
        %(Purchase_Date__c)s,
        %(Water_Source_Distance__c)s,
        %(Water_Source__c)s,
        %(leadcap__Facebook_Lead_ID__c)s,
        %(Payment_Terms__c)s,
        %(Acreage__c)s,
        %(Employee_ID__c)s,
        %(Employee_Name__c)s,
        %(Employee_Phone__c)s,
        %(Last_Assigned_Agent_Number__c)s,
        %(Through_Partner_Lead__c)s,
        %(Unique_Phone_Number__c)s,
        %(Through_Partner_Customer__c)s,
        %(Referral_Lead_ID__c)s,
        %(CDS1Tracker__c)s,
        %(CDS_Status__c)s,
        %(Survey_Stat__c)s,
        %(SADM_Account__c)s,
        %(SADM_CDS_ID__c)s,
        %(SADM_Customer__c)s,
        %(SADM_KYC_Date__c)s,
        %(SADM_CDS1_Date__c)s,
        %(SADM_CDS2_Date__c)s,
        %(SADM_Customer_Creation_Date__c)s,
        %(SADM_Deposit_Date__c)s,
        %(SADM_FIRST_MONTH_INSTALLMENT__c)s,
        %(SADM_JSF_Date__c)s,
        %(SADM_Status__c)s,
        %(Number_of_Units_Lead__c)s,
        %(KRA_Pin__c)s,
        %(Customer_to_Claim_VAT__c)s,
        %(Customer_Product_of_Interest__c)s,
        %(My_lead_Filter_c)s,
        %(Referral_Source_Application__c)s,
        %(Daily_Water_Usage__c)s,
        %(MobileNumberWithCountryCode__c)s,
        %(Lead_Source_Other_Comment__c)s,
        %(Total_Dynamic_Head__c)s,
        %(Custom_Opportunity_Name__c)s,
        %(SMSMessage__c)s,
        %(Contact_External_Id_Source__c)s,
        %(ContactRegionId__c)s,
        %(OpportunityPayPlanId__c)s,
        %(AMT_Customer_Name__c)s,
        %(Old_AMT_Customer_ID__c)s,
        %(Agent_Employee_Number__c)s,
        %(Other_Phone__c)s,
        %(Income_Threshold__c)s,
        %(Street)s,
        %(City)s,
        %(State)s,
        %(PostalCode)s;
        %(Title)s
        %(Latitude)s
        %(Longitude)s
        %(GeocodeAccuracy)s,
        %(Address)s,
        %(MasterRecordId)s
    )
    ON CONFLICT (Id) 
    DO UPDATE SET 
        LastModifiedDate = EXCLUDED.LastModifiedDate;
        LastActivityDate = EXCLUDED.LastActivityDate;
        Lead_Date_Created__c = EXCLUDED.Lead_Date_Created__c;
        SystemModstamp = EXCLUDED.SystemModstamp;
        CreatedById = EXCLUDED.CreatedById;
        LastModifiedById = EXCLUDED.LastModifiedById;
        Last_Updated_By__c = EXCLUDED.Last_Updated_By__c;
        FirstName = EXCLUDED.FirstName;
        LastName = EXCLUDED.LastName;
        Name = EXCLUDED.Name;
        Company = EXCLUDED.Company;
        Country = EXCLUDED.Country;
        Country_Code__c = EXCLUDED.Country_Code__c;
        Location__c = EXCLUDED.Location__c;
        Gender__c = EXCLUDED.Gender__c;
        Date_of_Birth__c = EXCLUDED.Date_of_Birth__c;
        Referral_ID__c = EXCLUDED.Referral_ID__c;
        Referral_Name__c = EXCLUDED.Referral_Name__c;
        Referral_Phone_Number__c = EXCLUDED.Referral_Phone_Number__c;
        Lead_Category__c = EXCLUDED.Lead_Category__c;
        Lead_Channel__c = EXCLUDED.Lead_Channel__c;
        Agent__c = EXCLUDED.Agent__c;
        Agent_Referral_SMSBody__c = EXCLUDED.Agent_Referral_SMSBody__c;
        Customer_Type__c = EXCLUDED.Customer_Type__c;
        IsDeleted = EXCLUDED.IsDeleted;
        Phone = EXCLUDED.Phone;
        MobilePhone = EXCLUDED.MobilePhone;
        Lead_AMT_Customer_Id__c = EXCLUDED.Lead_AMT_Customer_Id__c;
        Installation_Date__c = EXCLUDED.Installation_Date__c;
        LastViewedDate = EXCLUDED.LastViewedDate;
        LastReferencedDate = EXCLUDED.LastReferencedDate;
        Payment_Method__c = EXCLUDED.Payment_Method__c;
        Lead_Model_Category__c = EXCLUDED.Lead_Model_Category__c;
        ID_Number__c = EXCLUDED.ID_Number__c;
        Call_Back_Date__c = EXCLUDED.Call_Back_Date__c;
        Followup_Date__c = EXCLUDED.Followup_c;
        Product__c = EXCLUDED.Product__c;
        KYC_Status__c = EXCLUDED.KYC_Status__c;
        Agent_Phone_Number__c = EXCLUDED.Agent_Phone_Number__c;
        Preferred_Language__c = EXCLUDED.Preferred_Language__c;
        Product_del__c = EXCLUDED.Product_del__c;
        Purchase_Date__c = EXCLUDED.Purchase_Date__c;
        Water_Source_Distance__c = EXCLUDED.Water_Source_Distance__c;
        Water_Source__c = EXCLUDED.Water_Source__c;
        leadcap__Facebook_Lead_ID__c = EXCLUDED.Leadcap__Facebook_Lead_ID__c;
        Payment_Terms__c = EXCLUDED.Payment_Terms__c;
        Acreage__c = EXCLUDED.Acreage__c;
        Employee_ID__c = EXCLUDED.Employee_ID__c;
        Employee_Name__c = EXCLUDED.Employee_Name__c;
        Employee_Phone__c = EXCLUDED.Employee_Phone__c;
        Last_Assigned_Agent_Number__c = EXCLUDED.Last_Assigned_Agent_Number__c;
        Through_Partner_Lead__c = EXCLUDED.Through_Partner_Lead__c;
        Unique_Phone_Number__c = EXCLUDED.Unique_Phone_Number__c;
        Through_Partner_Customer__c = EXCLUDED.Through_Partner_Customer__c,
        Referral_Lead_ID__c = EXCLUDED.Referral_Lead_ID__c,
        CDS1Tracker__c = EXCLUDED.CDS1Tracker__c,
        CDS_Status__c = EXCLUDED.CDS_Status__c,
        Survey_Stat__c = EXCLUDED.Survey_Stat__c,
        SADM_Account__c = EXCLUDED.SADM_Account__c,
        SADM_CDS_ID__c = EXCLUDED.SADM_CDS_ID__c,
        SADM_Customer__c = EXCLUDED.SADM_Customer__c,
        SADM_KYC_Date__c = EXCLUDED.SADM_KYC_Date__c,
        SADM_CDS1_Date__c = EXCLUDED.SADM_CDS1_Date__c,
        SADM_CDS2_Date__c = EXCLUDED.SADM_CDS2_Date__c,
        SADM_Customer_Creation_Date__c = EXCLUDED.SADM_Customer_Creation_Date__c,
        SADM_Deposit_Date__c = EXCLUDED.SADM_Deposit_Date__c,
        SADM_FIRST_MONTH_INSTALLMENT__c = EXCLUDED.SADM_FIRST_MONTH_INSTALLMENT__c,
        SADM_JSF_Date__c = EXCLUDED.SADM_JSF_Date__c,
        SADM_Status__c = EXCLUDED.SADM_Status__c,
        Number_of_Units_Lead__c = EXCLUDED.Number_of_Units_Lead__c,
        KRA_Pin__c = EXCLUDED.KRA_Pin__c,
        Customer_to_Claim_VAT__c = EXCLUDED.Customer_to_Claim_VAT__c,
        Customer_Product_of_Interest__c = EXCLUDED.Customer_Product_of_Interest__c,
        My_lead_Filter_c = EXCLUDED.My_lead_Filter_c,
        Referral_Source_Application__c = EXCLUDED.Referral_Source_Application__c;
        Daily_Water_Usage__c = EXCLUDED.Daily_Water_Usage__c;
        MobileNumberWithCountryCode__c = EXCLUDED.MobileNumberWithCountryCode__c;
        Lead_Source_Other_Comment__c = EXCLUDED.Lead_Source_Other_Comment__c;
        Total_Dynamic_Head__c = EXCLUDED.Total_Dynamic_Head__c;
        Custom_Opportunity_Name__c = EXCLUDED.Custom_Opportunity_Name__c;
        SMSMessage__c = EXCLUDED.SMSMessage__c;
        Contact_External_Id_Source__c = EXCLUDED.Contact_External_Id_Source__c;
        ContactRegionId__c = EXCLUDED.ContactRegionId__c;
        OpportunityPayPlanId__c = EXCLUDED.OpportunityPayPlanId__c;
        AMT_Customer_Name__c = EXCLUDED.AMT_Customer_Name__c;
        Old_AMT_Customer_ID__c = EXCLUDED.Old_AMT_Customer_ID__c;
        Agent_Employee_Number__c = EXCLUDED.Agent_Employee_Number__c;
        Other_Phone__c = EXCLUDED.Other_Phone__c;
        Income_Threshold__c = EXCLUDED.Income_Threshold__c;
        Street = EXCLUDED.Street;
        City = EXCLUDED.City;
        State = EXCLUDED.State;
        PostalCode = EXCLUDED.PostalCode;
        Title = EXCLUDED.Title;
        Latitude = EXCLUDED.Latitude;
        Longitude = EXCLUDED.Longitude;
        GeocodeAccuracy = EXCLUDED.GeocodeAccuracy;
        Address = EXCLUDED.Address
        MasterRecordId = EXCLUDED.MasterRecordId;
    """
    try:
        # Prepare data
        data = [{
            # Map all fields here
            'Id': lead.get('Id'),
        } for lead in leads]
        execute_batch(cursor, query, data)
        db_connection.commit()
        print(f"Successfully upserted {len(leads)} leads")
    except Exception as e:
        db_connection.rollback()
        print(f"Error saving leads: {str(e)}")
    finally:
        cursor.close()
        db_connection.close()

if __name__ == "__main__":
    # 1. Get Salesforce access token
    auth_result = get_salesforce_token()
    
    if not auth_result:
        print("Authentication failed. Exiting.")
        exit(1)
        
    access_token, instance_url = auth_result
    
    # 2. Fetch leads from Salesforce
    print("\nFetching leads from Salesforce...")
    leads = fetch_salesforce_leads(access_token, instance_url)
    
    if not leads:
        print("No leads found or error occurred during fetch.")
        exit(2)
        
    print(f"Found {len(leads)} leads to process")
    
    # 3. Save to PostgreSQL
    print("\nSaving leads to PostgreSQL...")
    save_to_postgresql(leads)
    print("Process completed successfully!")