import os
import logging
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
from decimal import Decimal
from typing import Tuple, List, Dict, Any
from simple_salesforce import Salesforce  # New import
import clickhouse_connect
import pandas as pd
import csv
import io
import time  # For any future polling if needed

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

SF_CONFIGS = {
    "sf_auth_url": "https://login.salesforce.com/services/oauth2/token",
    "sf_client_id": os.getenv("sf_client_id"),
    "sf_client_secret": os.getenv("sf_client_secret"),
    "sf_username": os.getenv("sf_username"),
    "sf_password": os.getenv("sf_password") + os.getenv("sf_security_token", ""),  # Append token if present
    "request_timeout": 30,
    "sf_object_name": "Lead",
}

CLICKHOUSE_CONFIGS = {
    "clickhouse_host": os.getenv("SC_CH_CLOUD_HOST"),
    "clickhouse_port": int(os.getenv("SC_CH_CLOUD_PORT", 8443)),
    "clickhouse_user": os.getenv("SC_CH_CLOUD_USER"),
    "clickhouse_password": os.getenv("SC_CH_CLOUD_PASSWORD"),
    "clickhouse_database": os.getenv("SC_CH_CLOUD_DATABASE", "test"),
    "clickhouse_table": "test_lead",
}

# Field mapping dictionary - Assuming you have populated this with 129 fields as per log
SF_OBJECT_FIELDS = {
    "Id": "id",
    "IsDeleted": "boolean",
    "MasterRecordId": "reference",
    "LastName": "string",
    "FirstName": "string",
    "Salutation": "picklist",
    "Name": "string",
    "Title": "string",
    "Company": "string",
    "Street": "textarea",
    "City": "string",
    "State": "string",
    "PostalCode": "string",
    "Country": "string",
    "Latitude": "double",
    "Longitude": "double",
    "GeocodeAccuracy": "picklist",
    #"Address": "address",
    "Phone": "phone",
    "MobilePhone": "phone",
    "Website": "url",
    "PhotoUrl": "url",
    "LeadSource": "picklist",
    "Status": "picklist",
    "Industry": "picklist",
    "Rating": "picklist",
    "NumberOfEmployees": "int",
    "OwnerId": "reference",
    "IsConverted": "boolean",
    "ConvertedDate": "date",
    "ConvertedAccountId": "reference",
    "ConvertedContactId": "reference",
    "ConvertedOpportunityId": "reference",
    "IsUnreadByOwner": "boolean",
    "CreatedDate": "datetime",
    "CreatedById": "reference",
    "LastModifiedDate": "datetime",
    "LastModifiedById": "reference",
    "SystemModstamp": "datetime",
    "LastActivityDate": "date",
    "LastViewedDate": "datetime",
    "LastReferencedDate": "datetime",
    "Jigsaw": "string",
    "JigsawContactId": "string",
    "EmailBouncedReason": "string",
    "EmailBouncedDate": "datetime",
    "Acreage__c": "double",
    "Country_Code__c": "picklist",
    "Date_of_Birth__c": "date",
    "Gender__c": "picklist",
    "Lead_AMT_Customer_Id__c": "string",
    "Installation_Date__c": "date",
    "Lead_Category__c": "picklist",
    "Lead_Channel__c": "picklist",
    "Location__c": "string",
    "Payment_Method__c": "picklist",
    "Preferred_Language__c": "picklist",
    "Product_del__c": "picklist",
    "Purchase_Date__c": "picklist",
    "Water_Source_Distance__c": "double",
    "Water_Source__c": "picklist",
    "leadcap__Facebook_Lead_ID__c": "string",
    "Customer_Type__c": "picklist",
    "Lead_Model_Category__c": "string",
    "ID_Number__c": "string",
    "Call_Back_Date__c": "date",
    "Follow_Up_Date__c": "date",
    "Product__c": "reference",
    "KYC_Status__c": "picklist",
    "Agent_Phone_Number__c": "string",
    "Agent__c": "reference",
    "Payment_Terms__c": "picklist",
    "Referral_Name__c": "string",
    "Referral_ID__c": "string",
    "Income_Threshold__c": "currency",
    "Last_Updated_By__c": "reference",
    "Daily_Water_Usage__c": "double",
    "MobileNumberWithCountryCode__c": "string",
    "Lead_Source_Other_Comment__c": "textarea",
    "Total_Dynamic_Head__c": "double",
    "SmileIdentity_JSON__c": "textarea",
    "Referral_Phone_Number__c": "phone",
    "Custom_Opportunity_Name__c": "string",
    "SMSMessage__c": "textarea",
    "Contact_External_Id_Source__c": "string",
    "ContactRegionId__c": "string",
    "OpportunityPayPlanId__c": "string",
    "AMT_Customer_Name__c": "string",
    "Old_AMT_Customer_ID__c": "string",
    "Agent_Employee_Number__c": "string",
    "Other_Phone__c": "phone",
    "Lead_Date_Created__c": "datetime",
    "Number_of_Units_Lead__c": "double",
    "KRA_Pin__c": "string",
    "Customer_to_Claim_VAT__c": "picklist",
    "Customer_Product_of_Interest__c": "string",
    "My_lead_Filter__c": "boolean",
    "Referral_Source_Application__c": "picklist",
    "Agent_Referral_SMSBody__c": "textarea",
    "Through_Partner_Lead__c": "reference",
    "Unique_Phone_Number__c": "string",
    "Through_Partner_Customer__c": "reference",
    "Referral_Lead_ID__c": "phone",
    "CDS1Tracker__c": "picklist",
    "CDS_Status__c": "string",
    "Survey_Stat__c": "picklist",
    "SADM_Account__c": "reference",
    "SADM_CDS_ID__c": "reference",
    "SADM_Customer__c": "reference",
    "SADM_KYC_Date__c": "datetime",
    "SADM_CDS1_Date__c": "datetime",
    "SADM_CDS2_Date__c": "datetime",
    "SADM_Customer_Creation_Date__c": "datetime",
    "SADM_Deposit_Date__c": "datetime",
    "SADM_FIRST_MONTH_INSTALLMENT__c": "date",
    "SADM_JSF_Date__c": "date",
    "SADM_Status__c": "string",
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
def map_salesforce_to_clickhouse_type(sf_type: str) -> str:
    """Map Salesforce data types to ClickHouse data types."""
    type_mapping = {
        # String types
        'id': 'String',
        'string': 'String',
        'textarea': 'String',
        'picklist': 'String',
        'multipicklist': 'String',
        'phone': 'String',
        'email': 'String',
        'url': 'String',
        'reference': 'String',
        'address': 'String',
        
        # Numeric types - make nullable for empty values
        'double': 'Nullable(Float64)',
        'currency': 'Nullable(Decimal64(2))',
        'int': 'Nullable(Int32)',
        
        # Boolean types
        'boolean': 'Bool',
        
        # TEMPORARY FIX: Use String for all date/datetime to avoid conversion issues
        'date': 'String',
        'datetime': 'String',
        'time': 'String',
        
        # Special types
        'location': 'String',
        'multipicklist': 'String',  # Duplicate, but OK
    }
    
    return type_mapping.get(sf_type.lower(), 'String')  # Default to String


def normalize_field_name(field_name: str) -> str:
    """Normalize field name to lowercase with underscores."""
    normalized = field_name.strip().lower()
    normalized = normalized.replace(' ', '_').replace('-', '_').replace('.', '_')
    normalized = normalized.replace('(', '').replace(')', '').replace('%', 'percent')
    normalized = normalized.replace('&', 'and').replace('@', 'at').replace('#', 'number')
    
    # Remove multiple consecutive underscores and trim
    while '__' in normalized:
        normalized = normalized.replace('__', '_')
    normalized = normalized.strip('_')
    
    return normalized

# Static ranges from our analysis - Updated with proper ISO datetime literals (.000Z start, .999Z end for inclusivity)
# Static ranges from our analysis - Updated with proper ISO datetime literals (.000Z start, .999Z end for inclusivity)
DATE_RANGES = {
    '2022_Q1': {
        'start': '2022-01-01T00:00:00.000Z',
        'end': '2022-03-31T23:59:59.999Z',
        'estimated_count': 44524
    },
    '2022_Q2': {
        'start': '2022-04-01T00:00:00.000Z',
        'end': '2022-06-30T23:59:59.999Z',
        'estimated_count': 44524
    },
    '2022_Q3': {
        'start': '2022-07-01T00:00:00.000Z',
        'end': '2022-09-30T23:59:59.999Z',
        'estimated_count': 44524
    },
    '2022_Q4': {
        'start': '2022-10-01T00:00:00.000Z',
        'end': '2022-12-31T23:59:59.999Z',
        'estimated_count': 44524
    },
    '2024_Q1': {
        'start': '2024-01-01T00:00:00.000Z',
        'end': '2024-03-31T23:59:59.999Z',
        'estimated_count': 47380
    },
    '2024_Q2': {
        'start': '2024-04-01T00:00:00.000Z',
        'end': '2024-06-30T23:59:59.999Z',
        'estimated_count': 47380
    },
    '2024_Q3': {
        'start': '2024-07-01T00:00:00.000Z',
        'end': '2024-09-30T23:59:59.999Z',
        'estimated_count': 47380
    },
    '2024_Q4': {
        'start': '2024-10-01T00:00:00.000Z',
        'end': '2024-12-31T23:59:59.999Z',
        'estimated_count': 47380
    },
    '2025_Q1': {
        'start': '2025-01-01T00:00:00.000Z',
        'end': '2025-03-31T23:59:59.999Z',
        'estimated_count': 30472
    },
    '2025_Q2': {
        'start': '2025-04-01T00:00:00.000Z',
        'end': '2025-06-30T23:59:59.999Z',
        'estimated_count': 30472
    },
    '2025_Q3': {
        'start': '2025-07-01T00:00:00.000Z',
        'end': '2025-09-30T23:59:59.999Z',
        'estimated_count': 30472
    },
    '2025_Q4': {  # Will be capped dynamically to current date
        'start': '2025-10-01T00:00:00.000Z',
        'end': '2025-12-31T23:59:59.999Z',
        'estimated_count': 30472
    },
    '2023_Q1': {'start': '2023-01-01T00:00:00.000Z', 'end': '2023-03-31T23:59:59.999Z', 'estimated_count': 21855},
    '2023_Q2': {'start': '2023-04-01T00:00:00.000Z', 'end': '2023-06-30T23:59:59.999Z', 'estimated_count': 21855},
    '2023_Q3': {'start': '2023-07-01T00:00:00.000Z', 'end': '2023-09-30T23:59:59.999Z', 'estimated_count': 21855},
    '2023_Q4': {'start': '2023-10-01T00:00:00.000Z', 'end': '2023-12-31T23:59:59.999Z', 'estimated_count': 21855},
}

def validate_configs():
    """Validate all required configurations."""
    errors = []
    
    # Validate Salesforce config
    required_sf_keys = ["sf_client_id", "sf_client_secret", "sf_username", "sf_password", "sf_auth_url"]
    missing_sf = [key for key in required_sf_keys if not SF_CONFIGS.get(key)]
    if missing_sf:
        errors.append(f"Missing Salesforce config: {missing_sf}")
    
    # Validate ClickHouse config
    required_ch_vars = ["SC_CH_CLOUD_HOST", "SC_CH_CLOUD_USER", "SC_CH_CLOUD_PASSWORD"]
    missing_ch = [var for var in required_ch_vars if not os.getenv(var)]
    if missing_ch:
        errors.append(f"Missing ClickHouse env vars: {missing_ch}")
    
    # Validate field mapping
    if len(SF_OBJECT_FIELDS) < 5:  # Arbitrary threshold; adjust as needed
        errors.append("SF_OBJECT_FIELDS is incomplete! Add all Lead fields and types.")
    
    if errors:
        raise ValueError("Configuration errors:\n- " + "\n- ".join(errors))
    
    logger.info("✅ All configurations validated successfully")

def get_salesforce_token() -> Tuple[str, str]:
    """Fetch OAuth token from Salesforce."""
    auth_url = SF_CONFIGS["sf_auth_url"]
    client_id = SF_CONFIGS["sf_client_id"]
    client_secret = SF_CONFIGS["sf_client_secret"]
    username = SF_CONFIGS["sf_username"]
    password = SF_CONFIGS["sf_password"]
    timeout = SF_CONFIGS["request_timeout"]
    
    payload = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password
    }
    
    try:
        logger.info("🔐 Authenticating with Salesforce...")
        response = requests.post(auth_url, data=payload, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        access_token = data["access_token"]
        instance_url = data["instance_url"].strip("/")
        
        logger.info("✅ Successfully authenticated with Salesforce!")
        return access_token, instance_url
        
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
        raise

def get_sf_connection() -> Salesforce:
    """Initialize simple-salesforce connection using OAuth token."""
    access_token, instance_url = get_salesforce_token()
    
    try:
        sf = Salesforce(session_id=access_token, instance_url=instance_url)  # Fixed: instance_url, not sf_instance_url
        logger.info(f"✅ Connected to Salesforce (API version: {sf.sf_version})")
        return sf
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Salesforce: {e}")
        raise

def get_fields_with_types() -> List[Dict[str, str]]:
    """Get field information from our static mapping."""
    fields_with_types = []
    
    for original_name, sf_type in SF_OBJECT_FIELDS.items():
        field_info = {
            'original_name': original_name,
            'normalized_name': normalize_field_name(original_name),
            'salesforce_type': sf_type,
            'clickhouse_type': map_salesforce_to_clickhouse_type(sf_type)
        }
        fields_with_types.append(field_info)
    
    logger.info(f"✅ Loaded {len(fields_with_types)} fields from static mapping")
    return fields_with_types

def create_clickhouse_table_if_not_exists(ch_client, fields_with_types: List[Dict[str, str]]):
    """Create ClickHouse table with proper data types."""
    database = CLICKHOUSE_CONFIGS["clickhouse_database"]
    table_name = CLICKHOUSE_CONFIGS["clickhouse_table"]
    
    # Generate column definitions
    columns = []
    for field in fields_with_types:
        column_def = f"`{field['normalized_name']}` {field['clickhouse_type']}"
        columns.append(column_def)
    
    # Add metadata columns (note: _salesforce_id duplicates 'id' but kept for clarity)
    columns.append("_synced_at DateTime DEFAULT now()")
    columns.append("_salesforce_id String")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database}.{table_name} (
        {', '.join(columns)}
    ) ENGINE = MergeTree()
    ORDER BY (_synced_at, _salesforce_id)
    """
    
    # Use command() instead of execute()
    ch_client.command(create_table_sql)
    logger.info(f"✅ Table {database}.{table_name} created with {len(columns)} columns")

def fetch_date_range(sf: Salesforce, soql_query: str, fields_with_types: List[Dict[str, str]], range_name: str) -> List[Dict[str, Any]]:
    """Fetch data for a specific date range using simple-salesforce Bulk API."""
    range_results = []
    
    try:
        logger.info(f"📋 {range_name}: Executing bulk query...")
        
        # Dynamic attribute access: sf.bulk.Lead -> getattr(sf.bulk, 'Lead')
        bulk_object = getattr(sf.bulk, SF_CONFIGS['sf_object_name'])
        all_results = bulk_object.query(soql_query)  # Returns flat list of dicts
        
        for record in all_results:
            processed_record = {}
            # Map fields (values are already strings from SF)
            for field in fields_with_types:
                orig_name = field['original_name']
                norm_name = field['normalized_name']
                value = record.get(orig_name, '')
                processed_record[norm_name] = value
            
            # Add Salesforce ID
            processed_record['_salesforce_id'] = record.get('Id', '')
            range_results.append(processed_record)
        
        logger.info(f"📦 {range_name}: Fetched {len(range_results):,} records")
        return range_results
        
    except Exception as e:
        logger.error(f"❌ {range_name}: Bulk query failed: {e}")
        # Optional: Log SOQL for debug
        logger.error(f"   SOQL: {soql_query[:200]}...")  # Truncated if long
        return []

def fetch_and_sync_by_date_ranges(sf: Salesforce, fields_with_types: List[Dict[str, str]]):
    """Fetch and sync data using our optimized date ranges."""
    try:
        # Initialize ClickHouse client once
        ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIGS["clickhouse_host"],
            port=CLICKHOUSE_CONFIGS["clickhouse_port"],
            username=CLICKHOUSE_CONFIGS["clickhouse_user"],
            password=CLICKHOUSE_CONFIGS["clickhouse_password"],
            database=CLICKHOUSE_CONFIGS["clickhouse_database"],
            secure=True
        )
        
        # Ensure table exists
        create_clickhouse_table_if_not_exists(ch_client, fields_with_types)
        
        field_list = ", ".join([field['original_name'] for field in fields_with_types])
        total_synced = 0
        
        # Process each date range
        for range_name, range_data in DATE_RANGES.items():
            start_date = range_data['start']
            end_date = range_data['end']
            estimated_count = range_data['estimated_count']
            
            # Dynamic cap for current quarter (2025_Q4)
            if range_name == '2025_Q4':
                now_str = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.999Z')
                if now_str < end_date:
                    end_date = now_str
                    logger.info(f"🔄 Capping {range_name} end to current time: {now_str[:10]}")
            
            logger.info(f"🔄 Processing {range_name}: {start_date.split('T')[0]} to {end_date.split('T')[0]} (~{estimated_count:,} records)")
            
            # Build SOQL query for this range (single line to avoid parsing issues)
            soql_query = f"SELECT {field_list} FROM {SF_CONFIGS['sf_object_name']} WHERE CreatedDate >= {start_date} AND CreatedDate <= {end_date} ORDER BY CreatedDate"
            logger.info(f"📝 SOQL query length: {len(soql_query)} characters")
            # Uncomment below for full query debug (may be very long)
            # logger.debug(f"Full SOQL: {soql_query}")
            
            # Fetch this range
            range_results = fetch_date_range(sf, soql_query, fields_with_types, range_name)
            
            # Sync this range to ClickHouse
            if range_results:
                range_synced = sync_range_to_clickhouse(ch_client, range_results, fields_with_types)
                total_synced += range_synced
                logger.info(f"✅ {range_name}: Synced {range_synced:,} records (Total: {total_synced:,})")
            else:
                logger.info(f"📭 {range_name}: No records found")
            
            # Memory cleanup between ranges
            import gc
            gc.collect()
        
        logger.info(f"🎉 Completed! Total records synced: {total_synced:,}")
        
    except Exception as e:
        logger.error(f"💥 Sync failed: {e}")
        raise

def convert_value(value: str, clickhouse_type: str):
    """Convert string values to appropriate Python types for ClickHouse."""
    # Handle empty/null values
    if value == '' or value is None or value == 'null':
        if clickhouse_type.startswith('Nullable'):
            return None
        elif clickhouse_type == 'Bool':
            return False
        elif clickhouse_type in ['Float64', 'Int32', 'Int64']:
            return 0
        elif clickhouse_type.startswith('Decimal'):
            return Decimal('0')
        else:
            return ''
    
    try:
        # Boolean - handle both bool objects and strings
        if clickhouse_type == 'Bool':
            if isinstance(value, bool):
                return value  # Already a boolean
            return str(value).lower() in ('true', '1', 'yes')
        
        # Numeric types
        elif clickhouse_type == 'Nullable(Float64)':
            return float(value) if value != '' else None
        elif clickhouse_type in ['Nullable(Int32)', 'Int32', 'Int64']:
            return int(float(value)) if value != '' else None
        elif clickhouse_type.startswith('Decimal') or clickhouse_type.startswith('Nullable(Decimal'):
            return Decimal(str(value)) if value != '' else None
        
        # Everything else (including String, date, datetime) - return as string
        else:
            return str(value)
            
    except (ValueError, TypeError) as e:
        logger.warning(f"⚠️  Failed to convert value '{value}' to {clickhouse_type}: {e}")
        # Return safe defaults
        if clickhouse_type.startswith('Nullable'):
            return None
        elif clickhouse_type in ['Float64', 'Int32', 'Int64']:
            return 0
        elif clickhouse_type == 'Bool':
            return False
        else:
            return str(value)

def sync_range_to_clickhouse(ch_client, range_data: List[Dict[str, Any]], fields_with_types: List[Dict[str, str]]) -> int:
    """Sync a single date range to ClickHouse."""
    database = CLICKHOUSE_CONFIGS["clickhouse_database"]
    table_name = CLICKHOUSE_CONFIGS["clickhouse_table"]
    
    # Create field mapping
    field_type_map = {field['normalized_name']: field['clickhouse_type'] for field in fields_with_types}
    field_type_map['_salesforce_id'] = 'String'
    normalized_fields = [field['normalized_name'] for field in fields_with_types]
    normalized_fields.append('_salesforce_id')
    
    # Convert records
    records = []
    for record in range_data:
        processed_record = {
            field: convert_value(record.get(field, ''), field_type_map.get(field, 'String'))
            for field in normalized_fields
        }
        records.append(processed_record)
    
    # Insert to ClickHouse
    if records:
        df = pd.DataFrame(records)
        ch_client.insert_df(f"{database}.{table_name}", df)
        return len(records)
    
    return 0

def main():
    """Main function with optimized date-range sync."""
    try:
        logger.info("🚀 Starting Salesforce to ClickHouse sync with optimized date ranges...")
        
        validate_configs()
        fields_with_types = get_fields_with_types()
        sf = get_sf_connection()  # New: Get simple-salesforce connection
        
        # Use the optimized date-range approach
        fetch_and_sync_by_date_ranges(sf, fields_with_types)
        
        logger.info("🎉 Sync completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Sync failed: {e}")
        raise

if __name__ == "__main__":
    main()