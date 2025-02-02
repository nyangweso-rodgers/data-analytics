from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import requests
import os
import csv
import io
import psycopg2
from psycopg2.extras import execute_batch
from salesforce_bulk import SalesforceBulk
from simple_salesforce import Salesforce
from tenacity import retry, stop_after_attempt, wait_exponential

# Load environment variables
load_dotenv()

# Global variable to store access token and expiry time
access_token = None
token_expiry = None

# Salesforce Authentication URL
sf_instance_url = os.getenv("sf_instance_url")

# PostgreSQL configuration
ep_stage_db_config = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}
def get_salesforce_token():
    """Fetch OAuth token from Salesforce."""
    sf_auth_url = "https://login.salesforce.com/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": os.getenv("sf_client_id"),
        "client_secret": os.getenv("sf_client_secret"),
        "username": os.getenv("sf_username"),
        "password": os.getenv("sf_password") #+ os.getenv("sf_security_token")
    }
    response = requests.post(sf_auth_url, data=payload)
    
    if response.status_code == 200:
        data = response.json()
        return data["access_token"], data["instance_url"]
    else:
        print(f"Auth failed: {response.text}")
        exit(1)
def get_salesforce_bulk():
    """Initialize Salesforce Bulk API connection using OAuth token."""
    global access_token, token_expiry  # Add global declaration
    access_token, instance_url = get_salesforce_token()
    
    # Extract domain from instance URL (remove "https://")
    domain = instance_url.replace("https://", "")
    
    return SalesforceBulk(
        sessionId=access_token,
        host=domain  # e.g., "na1.salesforce.com"
    )

# Date range configuration
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime.today()
DATE_CHUNK_MONTHS = 9  # Split into ~9 month chunks for 4 batches

def generate_date_ranges():
    """Generate date ranges in 9-month chunks from 2022-2025"""
    current = START_DATE
    ranges = []
    
    while current < END_DATE:
        chunk_end = current + timedelta(days=30*DATE_CHUNK_MONTHS)
        if chunk_end > END_DATE:
            chunk_end = END_DATE
        ranges.append((
            current.strftime("%Y-%m-%dT%H:%M:%SZ"),
            chunk_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        ))
        current = chunk_end + timedelta(days=1)  # Avoid overlap
    
    return ranges

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def process_date_range(bulk, start_date, end_date):
    print(f"Using access token: {access_token[:10]}...")  # Debug token
    """Process a single date range batch"""
    query = f"""
        SELECT Id,
        CreatedDate,LastModifiedDate,LastActivityDate,Lead_Date_Created__c,SystemModstamp,
        CreatedById,LastModifiedById,Last_Updated_By__c,
        FirstName,LastName,Name,
        Company,Country,Country_Code__c,Location__c,
        Gender__c,Date_of_Birth__c,
        Referral_ID__c,Referral_Name__c,Referral_Phone_Number__c,
        Lead_Category__c,Lead_Channel__c,
        Agent__c,Agent_Referral_SMSBody__c,
        Customer_Type__c,
        IsDeleted,
        Phone,MobilePhone,
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
        Water_Source_Distance__c,Water_Source__c,
        leadcap__Facebook_Lead_ID__c,
        Payment_Terms__c,
        Acreage__c,
        Employee_ID__c,Employee_Name__c,Employee_Phone__c,
        Last_Assigned_Agent_Number__c,
        Through_Partner_Lead__c,
        Unique_Phone_Number__c,
        Through_Partner_Customer__c,
        Referral_Lead_ID__c,
        CDS1Tracker__c,CDS_Status__c,
        Survey_Stat__c,
        SADM_Account__c,SADM_CDS_ID__c,SADM_Customer__c,SADM_KYC_Date__c,SADM_CDS1_Date__c,SADM_CDS2_Date__c,SADM_Customer_Creation_Date__c,SADM_Deposit_Date__c,SADM_FIRST_MONTH_INSTALLMENT__c,SADM_JSF_Date__c,SADM_Status__c,
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
        Contact_External_Id_Source__c,ContactRegionId__c,
        OpportunityPayPlanId__c,
        AMT_Customer_Name__c,
        Old_AMT_Customer_ID__c,
        Agent_Employee_Number__c,
        Other_Phone__c,
        Income_Threshold__c,
        Street,City,State,PostalCode,
        Title,
        MasterRecordId
        FROM Lead
        WHERE CreatedDate >= '{start_date}'
    AND CreatedDate < '{end_date}'
        ORDER BY CreatedDate
    """
    
    print(f"Processing {start_date} to {end_date}")
    total_processed = 0
    
    try:
        job = bulk.create_query_job("Lead", contentType='CSV')
        #job_id = job['id']
        job_id = job.id  # Use attribute access instead of dictionary
        
        # Add this check after creating the job:
        if not job or 'id' not in job:
            raise Exception(f"Failed to create query job: {job}")

        batch = bulk.query(job, query)
        bulk_url = f"{sf_instance_url}/services/data/v56.0/jobs/query/{job_id}" #Construct URL

        # Correct polling for job completion using requests
        while True:
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(bulk_url, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            job_info = response.json()
            state = job_info['state']
            print(f"Job Status: {state}")
            if state == 'Closed':
                break
            elif state == 'Failed':
                error_message = job_info.get('errorMessage', 'No error message')
                raise Exception(f"Salesforce Bulk Job Failed: {error_message}")
            time.sleep(5)

        for result in bulk.get_all_results_for_query_batch(batch):
            content = result.content.decode('utf-8')
            print(f"Batch Content: {content[:500]}")  # Print first 500 chars to inspect structure
            reader = csv.DictReader(io.StringIO(content))
            records = list(reader)

            if records:
                save_leads_batch(records)
                total_processed += len(records)
                print(f"Processed {len(records)} records (Total: {total_processed})")

        bulk.close_job(job)
        return total_processed
    
    except AttributeError:
        # Handle XML API format fallback
        job_id = job['id']

    except requests.exceptions.RequestException as e:
        print(f"Error checking job status: {e}")
        if hasattr(bulk, 'abort_job') and callable(bulk.abort_job) and 'job' in locals(): #check if bulk has the attribute abort_job and if job is defined before calling it
            bulk.abort_job(job)
        raise #Re-raise the exception to be handled by the retry decorator

    except Exception as e:
        print(f"Failed processing {start_date}-{end_date}: {str(e)}")
        if hasattr(bulk, 'abort_job') and callable(bulk.abort_job) and 'job' in locals(): #check if bulk has the attribute abort_job and if job is defined before calling it
            bulk.abort_job(job)
        raise
    
def save_leads_batch(records):
    """Save a batch of leads with UPSERT"""
    conn = psycopg2.connect(**ep_stage_db_config)
    cursor = conn.cursor()
    
    try:
        query = """
            INSERT INTO sf_leads (
                Id, CreatedDate,LastModifiedDate,LastActivityDate,Lead_Date_Created__c,SystemModstamp,
                CreatedById,LastModifiedById,Last_Updated_By__c,
                FirstName,LastName,Name,
                Company,Country,Country_Code__c,Location__c,
                Gender__c,Date_of_Birth__c,
                Referral_ID__c,Referral_Name__c,Referral_Phone_Number__c,
                Lead_Category__c,Lead_Channel__c,
                Agent__c,Agent_Referral_SMSBody__c,
                Customer_Type__c,
                IsDeleted,
                Phone,MobilePhone,
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
                Employee_ID__c,Employee_Name__c,Employee_Phone__c,
                Last_Assigned_Agent_Number__c,
                Through_Partner_Lead__c,
                Unique_Phone_Number__c,
                Through_Partner_Customer__c,
                Referral_Lead_ID__c,
                CDS1Tracker__c,CDS_Status__c,
                Survey_Stat__c,
                SADM_Account__c,SADM_CDS_ID__c,SADM_Customer__c,SADM_KYC_Date__c,SADM_CDS1_Date__c,SADM_CDS2_Date__c,SADM_Customer_Creation_Date__c,SADM_Deposit_Date__c,SADM_FIRST_MONTH_INSTALLMENT__c,SADM_JSF_Date__c,SADM_Status__c,
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
                Street,City,State,PostalCode,
                Title,MasterRecordId
            )
            VALUES %s
            ON CONFLICT (Id) DO UPDATE SET
                LastModifiedDate = EXCLUDED.LastModifiedDate,LastActivityDate = EXCLUDED.LastActivityDate,
                Lead_Date_Created__c = EXCLUDED.Lead_Date_Created__c,
                SystemModstamp = EXCLUDED.SystemModstamp,
                CreatedById = EXCLUDED.CreatedById,
                LastModifiedById = EXCLUDED.LastModifiedById,
                Last_Updated_By__c = EXCLUDED.Last_Updated_By__c,
                FirstName = EXCLUDED.FirstName,LastName = EXCLUDED.LastName,Name = EXCLUDED.Name,
                Company = EXCLUDED.Company,Country = EXCLUDED.Country,Country_Code__c = EXCLUDED.Country_Code__c,Location__c = EXCLUDED.Location__c,
                Gender__c = EXCLUDED.Gender__c,Date_of_Birth__c = EXCLUDED.Date_of_Birth__c,
                Referral_ID__c = EXCLUDED.Referral_ID__c,Referral_Name__c = EXCLUDED.Referral_Name__c, Referral_Phone_Number__c = EXCLUDED.Referral_Phone_Number__c,
                Lead_Category__c = EXCLUDED.Lead_Category__c,Lead_Channel__c = EXCLUDED.Lead_Channel__c,
                Agent__c = EXCLUDED.Agent__c,Agent_Referral_SMSBody__c = EXCLUDED.Agent_Referral_SMSBody__c,
                Customer_Type__c = EXCLUDED.Customer_Type__c,
                IsDeleted = EXCLUDED.IsDeleted,
                Phone = EXCLUDED.Phone,MobilePhone = EXCLUDED.MobilePhone,
                Lead_AMT_Customer_Id__c = EXCLUDED.Lead_AMT_Customer_Id__c,
                Installation_Date__c = EXCLUDED.Installation_Date__c,
                LastViewedDate = EXCLUDED.LastViewedDate,LastReferencedDate = EXCLUDED.LastReferencedDate,
                Payment_Method__c = EXCLUDED.Payment_Method__c,
                Lead_Model_Category__c = EXCLUDED.Lead_Model_Category__c,
                ID_Number__c = EXCLUDED.ID_Number__c,
                Call_Back_Date__c = EXCLUDED.Call_Back_Date__c,
                Follow_Up_Date__c = EXCLUDED.Follow_Up_Date__c,
                Product__c = EXCLUDED.Product__c,
                KYC_Status__c = EXCLUDED.KYC_Status__c,
                Agent_Phone_Number__c = EXCLUDED.Agent_Phone_Number__c,
                Preferred_Language__c = EXCLUDED.Preferred_Language__c,
                Product_del__c = EXCLUDED.Product_del__c,
                Purchase_Date__c = EXCLUDED.Purchase_Date__c,
                Water_Source_Distance__c = EXCLUDED.Water_Source_Distance__c,
                Water_Source__c = EXCLUDED.Water_Source__c,
                leadcap__Facebook_Lead_ID__c = EXCLUDED.Leadcap__Facebook_Lead_ID__c,
                Payment_Terms__c = EXCLUDED.Payment_Terms__c,
                Acreage__c = EXCLUDED.Acreage__c,
                Employee_ID__c = EXCLUDED.Employee_ID__c,
                Employee_Name__c = EXCLUDED.Employee_Name__c,
                Employee_Phone__c = EXCLUDED.Employee_Phone__c,
                Last_Assigned_Agent_Number__c = EXCLUDED.Last_Assigned_Agent_Number__c,
                Through_Partner_Lead__c = EXCLUDED.Through_Partner_Lead__c,
                Unique_Phone_Number__c = EXCLUDED.Unique_Phone_Number__c,
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
                Referral_Source_Application__c = EXCLUDED.Referral_Source_Application__c,
                Daily_Water_Usage__c = EXCLUDED.Daily_Water_Usage__c,
                MobileNumberWithCountryCode__c = EXCLUDED.MobileNumberWithCountryCode__c,
                Lead_Source_Other_Comment__c = EXCLUDED.Lead_Source_Other_Comment__c,
                Total_Dynamic_Head__c = EXCLUDED.Total_Dynamic_Head__c,
                Custom_Opportunity_Name__c = EXCLUDED.Custom_Opportunity_Name__c,
                SMSMessage__c = EXCLUDED.SMSMessage__c,
                Contact_External_Id_Source__c = EXCLUDED.Contact_External_Id_Source__c,
                ContactRegionId__c = EXCLUDED.ContactRegionId__c,
                OpportunityPayPlanId__c = EXCLUDED.OpportunityPayPlanId__c,
                AMT_Customer_Name__c = EXCLUDED.AMT_Customer_Name__c,
                Old_AMT_Customer_ID__c = EXCLUDED.Old_AMT_Customer_ID__c,
                Agent_Employee_Number__c = EXCLUDED.Agent_Employee_Number__c,
                Other_Phone__c = EXCLUDED.Other_Phone__c,
                Income_Threshold__c = EXCLUDED.Income_Threshold__c,
                Street = EXCLUDED.Street,
                City = EXCLUDED.City,
                State = EXCLUDED.State,
                PostalCode = EXCLUDED.PostalCode,
                Title = EXCLUDED.Title,
                MasterRecordId = EXCLUDED.MasterRecordId,
                ...
        """
        # Convert dictionaries to tuples: CRUCIAL STEP
        print(f"First record sample: {records[0]}")  # Add this for debugging
        values_to_insert = [tuple(record.values()) for record in records]  # Correct conversion

        execute_batch(cursor, query, values_to_insert) #Pass the values_to_insert
        conn.commit()
    except Exception as e:
        print(f"Error saving leads: {e}")
        conn.rollback() #Rollback in case of error
    finally:
        cursor.close()
        conn.close()

def main():
    try:
        bulk = get_salesforce_bulk()
        date_ranges = generate_date_ranges()
        
        for idx, (start, end) in enumerate(date_ranges, 1):
            print(f"\nProcessing batch {idx}/{len(date_ranges)}")
            try:
                # Test with small date range first
                if idx > 1: continue  # Process only first batch initially
                
                count = process_date_range(bulk, start, end)
                print(f"Batch {idx} completed: {count} records")
            except Exception as e:
                print(f"Batch {idx} failed: {str(e)}")
                if "InvalidBatch" in str(e):
                    print("Permanent failure - skipping batch")
                    continue
                raise
    finally:
        print("Cleanup completed")

if __name__ == "__main__":
    main()