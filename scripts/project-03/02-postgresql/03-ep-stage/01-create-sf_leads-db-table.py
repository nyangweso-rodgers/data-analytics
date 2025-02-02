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
            LastName VARCHAR(255),
            FirstName VARCHAR(255),
            Salutation VARCHAR(10),
            Name VARCHAR(255),
            Title VARCHAR(255),
            Company VARCHAR(255),
            Street VARCHAR(255),
            City VARCHAR(255),
            State VARCHAR(255),
            PostalCode VARCHAR(20),
            Country VARCHAR(255),
            Latitude FLOAT,
            Longitude FLOAT,
            GeocodeAccuracy VARCHAR(50),
            Address TEXT,
            Phone VARCHAR(20),
            MobilePhone VARCHAR(20),
            Website VARCHAR(255),
            PhotoUrl TEXT,
            LeadSource VARCHAR(255),
            Status VARCHAR(255),
            Industry VARCHAR(255),
            Rating VARCHAR(255),
            NumberOfEmployees INTEGER,
            OwnerId VARCHAR(18),
            IsConverted BOOLEAN,
            ConvertedDate TIMESTAMP,
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
            Jigsaw VARCHAR(255),
            JigsawContactId VARCHAR(255),
            EmailBouncedReason TEXT,
            EmailBouncedDate DATE,
            -- Custom Fields
            Acreage__c DECIMAL,
            Country_Code__c VARCHAR(10),
            Date_of_Birth__c DATE,
            Gender__c VARCHAR(50),
            Lead_AMT_Customer_Id__c VARCHAR(255),
            Installation_Date__c DATE,
            Lead_Category__c VARCHAR(255),
            Lead_Channel__c VARCHAR(255),
            Location__c VARCHAR(255),
            Payment_Method__c VARCHAR(255),
            Preferred_Language__c VARCHAR(50),
            Product_del__c VARCHAR(255),
            Purchase_Date__c DATE,
            Water_Source_Distance__c DECIMAL,
            Water_Source__c VARCHAR(255),
            leadcap__Facebook_Lead_ID__c VARCHAR(255),
            Customer_Type__c VARCHAR(255),
            Lead_Model_Category__c VARCHAR(255),
            ID_Number__c VARCHAR(255),
            Call_Back_Date__c DATE,
            Follow_Up_Date__c DATE,
            Product__c VARCHAR(255),
            KYC_Status__c VARCHAR(255),
            Agent_Phone_Number__c VARCHAR(20),
            Agent__c VARCHAR(255),
            Payment_Terms__c VARCHAR(255),
            Referral_Name__c VARCHAR(255),
            Referral_ID__c VARCHAR(255),
            Income_Threshold__c DECIMAL,
            Last_Updated_By__c VARCHAR(255),
            Daily_Water_Usage__c DECIMAL,
            MobileNumberWithCountryCode__c VARCHAR(20),
            Lead_Source_Other_Comment__c TEXT,
            Total_Dynamic_Head__c DECIMAL,
            SmileIdentity_JSON__c JSONB,
            Referral_Phone_Number__c VARCHAR(20),
            Custom_Opportunity_Name__c VARCHAR(255),
            SMSMessage__c TEXT,
            Contact_External_Id_Source__c VARCHAR(255),
            ContactRegionId__c VARCHAR(255),
            OpportunityPayPlanId__c VARCHAR(255),
            AMT_Customer_Name__c VARCHAR(255),
            Old_AMT_Customer_ID__c VARCHAR(255),
            Agent_Employee_Number__c VARCHAR(255),
            Other_Phone__c VARCHAR(20),
            Lead_Date_Created__c DATE,
            Number_of_Units_Lead__c INTEGER,
            KRA_Pin__c VARCHAR(255),
            Customer_to_Claim_VAT__c BOOLEAN,
            Customer_Product_of_Interest__c VARCHAR(255),
            My_lead_Filter__c VARCHAR(255),
            Referral_Source_Application__c VARCHAR(255),
            Agent_Referral_SMSBody__c TEXT,
            Through_Partner_Lead__c BOOLEAN,
            Unique_Phone_Number__c VARCHAR(20),
            Through_Partner_Customer__c BOOLEAN,
            Referral_Lead_ID__c VARCHAR(255),
            CDS1Tracker__c BOOLEAN,
            CDS_Status__c VARCHAR(255),
            Survey_Stat__c VARCHAR(255),
            SADM_Account__c VARCHAR(255),
            SADM_CDS_ID__c VARCHAR(255),
            SADM_Customer__c VARCHAR(255),
            SADM_KYC_Date__c DATE,
            SADM_CDS1_Date__c DATE,
            SADM_CDS2_Date__c DATE,
            SADM_Customer_Creation_Date__c DATE,
            SADM_Deposit_Date__c DATE,
            SADM_FIRST_MONTH_INSTALLMENT__c DECIMAL,
            SADM_JSF_Date__c DATE,
            SADM_Status__c VARCHAR(255),
            Employee_ID__c VARCHAR(255),
            Employee_Name__c VARCHAR(255),
            Employee_Phone__c VARCHAR(20),
            Last_Assigned_Agent_Number__c VARCHAR(20),
            CONSTRAINT unique_salesforce_id UNIQUE(Id)
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