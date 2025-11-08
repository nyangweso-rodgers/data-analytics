# Sync Lead From Salesforce to MySQL DB

# `V2` Script

- Sync Leads Object to MySQL from a specified date range.
- **Script Usage**

  - **Sync a predefined date range**

    ```py
        python sync_lead_to_mysql.py --date-range MTD
        python sync_lead_to_mysql.py --date-range 2025_Q3
    ```

  - **Sync all predefined ranges**

    ```py
        python sync_lead_to_mysql.py --date-range ALL
    ```

  - **Sync custom date range**

    ```py
        python sync_lead_to_mysql.py --start-date 2025-01-01 --end-date 2025-01-31
        python sync_lead_to_mysql.py --start-date 2025-01-01T00:00:00 --end-date 2025-10-31T23:59:59
    ```

  - **List available ranges**

    ```py
        python sync_lead_to_mysql.py --list-ranges
    ```

  - **Dry run** (**preview without syncing**)

    ```py
        python sync_lead_to_mysql.py --date-range MTD --dry-run
    ```

  - **View help**

    ```py
        python sync_lead_to_mysql.py --help
    ```

# Define Date Range

```py
# Predefined date ranges
DATE_RANGES = {
    '2022_Q1': {'start': '2022-01-01T00:00:00.000Z', 'end': '2022-03-31T23:59:59.999Z'},
    '2022_Q2': {'start': '2022-04-01T00:00:00.000Z', 'end': '2022-06-30T23:59:59.999Z'},
    '2022_Q3': {'start': '2022-07-01T00:00:00.000Z', 'end': '2022-09-30T23:59:59.999Z'},
    '2022_Q4': {'start': '2022-10-01T00:00:00.000Z', 'end': '2022-12-31T23:59:59.999Z'},

    '2023_Q1': {'start': '2023-01-01T00:00:00.000Z', 'end': '2023-03-31T23:59:59.999Z'},
    '2023_Q2': {'start': '2023-04-01T00:00:00.000Z', 'end': '2023-06-30T23:59:59.999Z'},
    '2023_Q3': {'start': '2023-07-01T00:00:00.000Z', 'end': '2023-09-30T23:59:59.999Z'},
    '2023_Q4': {'start': '2023-10-01T00:00:00.000Z', 'end': '2023-12-31T23:59:59.999Z'},

    '2024_Q1': {'start': '2024-01-01T00:00:00.000Z', 'end': '2024-03-31T23:59:59.999Z'},
    '2024_Q2': {'start': '2024-04-01T00:00:00.000Z', 'end': '2024-06-30T23:59:59.999Z'},
    '2024_Q3': {'start': '2024-07-01T00:00:00.000Z', 'end': '2024-09-30T23:59:59.999Z'},
    '2024_Q4_M1': {'start': '2024-10-01T00:00:00.000Z', 'end': '2024-10-31T23:59:59.999Z'},
    '2024_Q4_M2': {'start': '2024-11-01T00:00:00.000Z', 'end': '2024-11-30T23:59:59.999Z'},
    '2024_Q4_M3': {'start': '2024-12-01T00:00:00.000Z', 'end': '2024-12-31T23:59:59.999Z'},

    '2025_Q1': {'start': '2025-01-01T00:00:00.000Z', 'end': '2025-03-31T23:59:59.999Z'},
    '2025_Q2': {'start': '2025-04-01T00:00:00.000Z', 'end': '2025-06-30T23:59:59.999Z'},
    '2025_Q3': {'start': '2025-07-01T00:00:00.000Z', 'end': '2025-09-30T23:59:59.999Z'},
    '2025_Q4': {'start': '2025-10-01T00:00:00.000Z', 'end': '2025-11-07T23:59:59.999Z'},  # Up to yesterday
```

# Data Mapping

```py
    SF_OBJECT_FIELDS = {
    "Id": "id",
    "IsDeleted": "boolean",
    #"MasterRecordId": "reference",
    "LastName": "string",
    "FirstName": "string",
    #"Salutation": "picklist",
    "Name": "string",
    #"Title": "string",
    #"Company": "string",
    #"Street": "textarea",
    #"City": "string",
    #"State": "string",
    #"PostalCode": "string",
    #"Country": "string",
    #"Latitude": "double",
    #"Longitude": "double",
    #"GeocodeAccuracy": "picklist",
    #"Phone": "phone",
    "MobilePhone": "phone",
    #"Website": "url",
    #"PhotoUrl": "url",
    "LeadSource": "picklist",
    "Status": "picklist",
    #"Industry": "picklist",
    #"Rating": "picklist",
    #"NumberOfEmployees": "int",
    #"OwnerId": "reference",
    "IsConverted": "boolean",
    "ConvertedDate": "date",
    "ConvertedAccountId": "reference",
    #"ConvertedContactId": "reference",
    #"ConvertedOpportunityId": "reference",
    #"IsUnreadByOwner": "boolean",
    "CreatedDate": "datetime",
    "CreatedById": "reference",
    "LastModifiedDate": "datetime",
    "LastModifiedById": "reference",
    #"SystemModstamp": "datetime",
    #"LastActivityDate": "date",
    #"LastViewedDate": "datetime",
    #"LastReferencedDate": "datetime",
    #"Jigsaw": "string",
    #"JigsawContactId": "string",
    #"EmailBouncedReason": "string",
    #"EmailBouncedDate": "datetime",
    "Acreage__c": "double",
    #"Country_Code__c": "picklist",
    "Date_of_Birth__c": "date",
    "Gender__c": "picklist",
    "Lead_AMT_Customer_Id__c": "string",
    #"Installation_Date__c": "date",
    "Lead_Category__c": "picklist",
    "Lead_Channel__c": "picklist",
    "Location__c": "string",
    "Payment_Method__c": "picklist",
    "Preferred_Language__c": "picklist",
    #"Product_del__c": "picklist",
    "Purchase_Date__c": "picklist",
    "Water_Source_Distance__c": "double",
    "Water_Source__c": "picklist",
    #"leadcap__Facebook_Lead_ID__c": "string",
    "Customer_Type__c": "picklist",
    #"Lead_Model_Category__c": "string",
    "ID_Number__c": "string",
    #"Call_Back_Date__c": "date",
    #"Follow_Up_Date__c": "date",
    #"Product__c": "reference",
    "KYC_Status__c": "picklist",
    "Agent_Phone_Number__c": "string",
    "Agent__c": "reference",
    #"Payment_Terms__c": "picklist",
    "Referral_Name__c": "string",
    "Referral_ID__c": "string",
    "Income_Threshold__c": "currency",
    #"Last_Updated_By__c": "reference",
    "Daily_Water_Usage__c": "double",
    "MobileNumberWithCountryCode__c": "string",
    #"Lead_Source_Other_Comment__c": "textarea",
    "Total_Dynamic_Head__c": "double",
    "SmileIdentity_JSON__c": "textarea",
    "Referral_Phone_Number__c": "phone",
    #"Custom_Opportunity_Name__c": "string",
    #"SMSMessage__c": "textarea",
    #"Contact_External_Id_Source__c": "string",
    #"ContactRegionId__c": "string",
    #"OpportunityPayPlanId__c": "string",
    "AMT_Customer_Name__c": "string",
    #"Old_AMT_Customer_ID__c": "string",
    "Agent_Employee_Number__c": "string",
    "Other_Phone__c": "phone",
    "Lead_Date_Created__c": "datetime",
    #"Number_of_Units_Lead__c": "double",
    "KRA_Pin__c": "string",
    #"Customer_to_Claim_VAT__c": "picklist",
    "Customer_Product_of_Interest__c": "string",
    #"My_lead_Filter__c": "boolean",
    "Referral_Source_Application__c": "picklist",
    #"Agent_Referral_SMSBody__c": "textarea",
    "Through_Partner_Lead__c": "reference",
    #"Unique_Phone_Number__c": "string",
    "Through_Partner_Customer__c": "reference",
    "Referral_Lead_ID__c": "phone",
    "CDS1Tracker__c": "picklist",
    "CDS_Status__c": "string",
    "Survey_Stat__c": "picklist",
    "SADM_Account__c": "reference",
    "SADM_CDS_ID__c": "reference",
    #"SADM_Customer__c": "reference",
    #"SADM_KYC_Date__c": "datetime",
    #"SADM_CDS1_Date__c": "datetime",
    #"SADM_CDS2_Date__c": "datetime",
    #"SADM_Customer_Creation_Date__c": "datetime",
    #"SADM_Deposit_Date__c": "datetime",
    #"SADM_FIRST_MONTH_INSTALLMENT__c": "date",
    #"SADM_JSF_Date__c": "date",
    #"SADM_Status__c": "string",
    "Employee_ID__c": "string",
    "Employee_Name__c": "string",
    "Employee_Phone__c": "string",
    "Is_Lead_Employed__c": "boolean",
    "Auto_Assignment_Date__c": "datetime",
    "ExtAgentShopName__c": "string",
    "ExtAgentReferral_Code__c": "string",
    "ExtAgentProvider_region__c": "string",
    "ExtAgentProvider_name__c": "string",
    "ExtAgentPhone_Number__c": "string",
    "ExtAgentName__c": "string",
    "ExtAgentID__c": "string"
}
```
