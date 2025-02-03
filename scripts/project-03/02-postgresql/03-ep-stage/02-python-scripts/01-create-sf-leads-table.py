from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import OperationalError

# Load environment variables
load_dotenv()  # Ensure this is called before accessing os.getenv

# Database connection parameters
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

# Debugging: Print to check if variables are loaded
print(ep_stage_db_params)

def create_sf_leads_postgresdb_table ():
    """Creates a sf_leads table in PostgreSQL if it does not exist."""
    connection = None
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(**ep_stage_db_params)
        cur = connection.cursor()
        
        # Create table SQL with your Salesforce fields
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sf_leads (
            Id VARCHAR(18) PRIMARY KEY,
IsDeleted BOOLEAN,
MasterRecordId VARCHAR(18),
LastName TEXT,
FirstName TEXT,
Salutation TEXT,
Name TEXT,
Title TEXT,
Company TEXT,
Street TEXT,
City TEXT,
State TEXT,
PostalCode TEXT,
Country TEXT,
Latitude DOUBLE PRECISION,
Longitude DOUBLE PRECISION,
GeocodeAccuracy TEXT,
Address TEXT,
Phone VARCHAR(15),
MobilePhone VARCHAR(15),
Website TEXT,
LeadSource TEXT,
Status TEXT,
Industry TEXT,
Rating TEXT,
NumberOfEmployees INTEGER,
OwnerId VARCHAR(18),
IsConverted BOOLEAN,
ConvertedDate DATE,
ConvertedAccountId VARCHAR(18),
ConvertedContactId VARCHAR(18),
ConvertedOpportunityId VARCHAR(18),
IsUnreadByOwner BOOLEAN,
CreatedDate TIMESTAMP,
CreatedById VARCHAR(18),
LastModifiedDate TIMESTAMP,
LastModifiedById VARCHAR(18),
SystemModstamp TIMESTAMP,
LastActivityDate DATE,
LastViewedDate TIMESTAMP,
LastReferencedDate TIMESTAMP,
Jigsaw TEXT,
JigsawContactId TEXT,
EmailBouncedReason TEXT,
EmailBouncedDate TIMESTAMP,
Acreage__c DOUBLE PRECISION,
Country_Code__c TEXT,
Date_of_Birth__c DATE,
Gender__c TEXT,
Lead_AMT_Customer_Id__c TEXT,
Installation_Date__c DATE,
Lead_Category__c TEXT,
Lead_Channel__c TEXT,
Location__c TEXT,
Payment_Method__c TEXT,
Preferred_Language__c TEXT,
Product_del__c TEXT,
Purchase_Date__c VARCHAR(18),
Water_Source_Distance__c DOUBLE PRECISION,
Water_Source__c TEXT,
leadcap__Facebook_Lead_ID__c TEXT,
Customer_Type__c TEXT,
Lead_Model_Category__c TEXT,
ID_Number__c TEXT,
Call_Back_Date__c DATE,
Follow_Up_Date__c DATE,
Product__c VARCHAR(18),
KYC_Status__c TEXT,
Agent_Phone_Number__c TEXT,
Agent__c VARCHAR(18),
Payment_Terms__c TEXT,
Referral_Name__c TEXT,
Referral_ID__c TEXT,
Income_Threshold__c NUMERIC(18,2),
Last_Updated_By__c VARCHAR(18),
Daily_Water_Usage__c DOUBLE PRECISION,
MobileNumberWithCountryCode__c TEXT,
Lead_Source_Other_Comment__c TEXT,
Total_Dynamic_Head__c DOUBLE PRECISION,
Referral_Phone_Number__c VARCHAR(15),
Custom_Opportunity_Name__c TEXT,
SMSMessage__c TEXT,
Contact_External_Id_Source__c TEXT,
ContactRegionId__c TEXT,
OpportunityPayPlanId__c TEXT,
AMT_Customer_Name__c TEXT,
Old_AMT_Customer_ID__c TEXT,
Agent_Employee_Number__c TEXT,
Other_Phone__c TEXT,
Lead_Date_Created__c TIMESTAMP,
Number_of_Units_Lead__c DOUBLE PRECISION,
KRA_Pin__c TEXT,
Customer_to_Claim_VAT__c TEXT,
Customer_Product_of_Interest__c TEXT,
My_lead_Filter__c BOOLEAN,
Referral_Source_Application__c TEXT,
Agent_Referral_SMSBody__c TEXT,
Through_Partner_Lead__c VARCHAR(18),
Unique_Phone_Number__c TEXT,
Through_Partner_Customer__c VARCHAR(18),
Referral_Lead_ID__c TEXT,
CDS1Tracker__c TEXT,
CDS_Status__c TEXT,
Survey_Stat__c TEXT,
SADM_Account__c VARCHAR(18),
SADM_CDS_ID__c VARCHAR(18),
SADM_Customer__c VARCHAR(18),
SADM_KYC_Date__c TIMESTAMP,
SADM_CDS1_Date__c TIMESTAMP,
SADM_CDS2_Date__c TIMESTAMP,
SADM_Customer_Creation_Date__c TIMESTAMP,
SADM_Deposit_Date__c TIMESTAMP,
SADM_FIRST_MONTH_INSTALLMENT__c DATE,
SADM_JSF_Date__c DATE,
SADM_Status__c TEXT,
Employee_ID__c TEXT,
Employee_Name__c TEXT,
Employee_Phone__c TEXT,
Last_Assigned_Agent_Number__c DOUBLE PRECISION
        );
        """
        # Execute the SQL command
        cur.execute(create_table_query)
        connection.commit()
        print("Table 'sf_leads' created successfully!")
    except OperationalError as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection:
            cur.close()
            connection.close()
            print("PostgreSQL connection is closed")
            
if __name__ == "__main__":
    create_sf_leads_postgresdb_table()