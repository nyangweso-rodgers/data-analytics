import os
import logging
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
from decimal import Decimal
from typing import Tuple, List, Dict, Any
from simple_salesforce import Salesforce
import mysql.connector
from mysql.connector import Error
import argparse
import sys
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

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
    "sf_password": os.getenv("sf_password") + os.getenv("sf_security_token", ""),
    "request_timeout": 30,
    "sf_object_name": "Lead",
}

MYSQL_CONFIGS = {
    "mysql_db_host": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_HOST"),
    "mysql_db_port": int(os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PORT", 3306)),
    "mysql_db_user": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_USER"),
    "mysql_db_password": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"),
    "mysql_db_name": "data-migration-staging",
    "mysql_target_table_name": "sf_lead_v14"
}

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
    'TODAY': {'start': '2025-11-08T00:00:00.000Z', 'end': '2025-11-08T23:59:59.999Z'}, # Today
}

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
    #"Acreage__c": "double",
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
    #"Water_Source__c": "picklist",
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
    #"Income_Threshold__c": "currency",
    #"Last_Updated_By__c": "reference",
    #"Daily_Water_Usage__c": "double",
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
    #"Referral_Source_Application__c": "picklist",
    #"Agent_Referral_SMSBody__c": "textarea",
    "Through_Partner_Lead__c": "reference",
    #"Unique_Phone_Number__c": "string",
    "Through_Partner_Customer__c": "reference",
    "Referral_Lead_ID__c": "phone",
    "CDS1Tracker__c": "picklist",
    "CDS_Status__c": "string",
    #"Survey_Stat__c": "picklist",
    #"SADM_Account__c": "reference",
    "SADM_CDS_ID__c": "reference",
    #"SADM_Customer__c": "reference",
    "SADM_KYC_Date__c": "datetime",
    "SADM_CDS1_Date__c": "datetime",
    "SADM_CDS2_Date__c": "datetime",
    #"SADM_Customer_Creation_Date__c": "datetime",
    #"SADM_Deposit_Date__c": "datetime",
    #"SADM_FIRST_MONTH_INSTALLMENT__c": "date",
    #"SADM_JSF_Date__c": "date",
    #"SADM_Status__c": "string",
    "Employee_ID__c": "string",
    #"Employee_Name__c": "string",
    "Employee_Phone__c": "string",
    "Is_Lead_Employed__c": "boolean",
    #"Auto_Assignment_Date__c": "datetime",
    #"ExtAgentShopName__c": "string",
    "ExtAgentReferral_Code__c": "string",
    "ExtAgentProvider_region__c": "string",
    "ExtAgentProvider_name__c": "string",
    "ExtAgentPhone_Number__c": "string",
    "ExtAgentName__c": "string",
    "ExtAgentID__c": "string"
}

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Sync Salesforce Lead data to MySQL on RDS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Sync a predefined date range
  python script.py --date-range MTD
  python script.py --date-range 2025_Q3
  
  # Sync custom date range
  python script.py --start-date 2025-01-01 --end-date 2025-01-31
  
  # Sync all predefined ranges
  python script.py --date-range ALL
  
  # List available predefined ranges
  python script.py --list-ranges
        """
    )
    
    parser.add_argument(
        '--date-range',
        type=str,
        help=f'Predefined date range to sync. Options: {", ".join(DATE_RANGES.keys())}, ALL'
    )
    
    parser.add_argument(
        '--start-date',
        type=str,
        help='Custom start date (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)'
    )
    
    parser.add_argument(
        '--end-date',
        type=str,
        help='Custom end date (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)'
    )
    
    parser.add_argument(
        '--list-ranges',
        action='store_true',
        help='List all available predefined date ranges and exit'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Display query details without executing sync'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records to insert per batch (default: 1000)'
    )
    
    return parser.parse_args()

def list_available_ranges():
    """Display all available predefined date ranges."""
    print("\n📅 Available Predefined Date Ranges:")
    print("=" * 70)
    for range_name, range_data in DATE_RANGES.items():
        start = range_data['start'].split('T')[0]
        end = range_data['end'].split('T')[0]
        print(f"  {range_name:15} : {start} to {end}")
    print("=" * 70)
    print("\nUsage: python script.py --date-range <range_name>")
    print("       python script.py --date-range ALL (to sync all ranges)\n")

def parse_custom_date(date_str: str) -> str:
    """Convert custom date string to Salesforce format."""
    try:
        if 'T' in date_str:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        else:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    except ValueError as e:
        logger.error(f"Invalid date format '{date_str}': {e}")
        logger.error("Expected format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
        sys.exit(1)

def map_salesforce_to_mysql_type(sf_type: str) -> str:
    """Map Salesforce data types to MySQL data types."""
    type_mapping = {
        'id': 'VARCHAR(18)',
        'string': 'TEXT',
        'textarea': 'TEXT',
        'picklist': 'VARCHAR(100)',
        'multipicklist': 'TEXT',
        'phone': 'VARCHAR(40)',
        'email': 'VARCHAR(80)',
        'url': 'TEXT',
        'reference': 'VARCHAR(18)',
        'address': 'TEXT',
        'double': 'DOUBLE',
        'currency': 'DECIMAL(18,2)',
        'int': 'INT',
        'boolean': 'TINYINT(1)',
        'date': 'DATE',
        'datetime': 'DATETIME',
        'time': 'TIME',
        'location': 'TEXT',
    }
    return type_mapping.get(sf_type.lower(), 'TEXT')

def normalize_field_name(field_name: str) -> str:
    """Normalize field name to lowercase with underscores."""
    normalized = field_name.strip().lower()
    normalized = normalized.replace(' ', '_').replace('-', '_').replace('.', '_')
    normalized = normalized.replace('(', '').replace(')', '').replace('%', 'percent')
    normalized = normalized.replace('&', 'and').replace('@', 'at').replace('#', 'number')
    
    while '__' in normalized:
        normalized = normalized.replace('__', '_')
    normalized = normalized.strip('_')
    
    return normalized

def validate_configs():
    """Validate all required configurations."""
    errors = []
    
    required_sf_keys = ["sf_client_id", "sf_client_secret", "sf_username", "sf_password", "sf_auth_url"]
    missing_sf = [key for key in required_sf_keys if not SF_CONFIGS.get(key)]
    if missing_sf:
        errors.append(f"Missing Salesforce config: {missing_sf}")
    
    required_mysql_vars = ["SC_SALES_SERVICE_DEV_MYSQL_DB_HOST", "SC_SALES_SERVICE_DEV_MYSQL_DB_USER", 
                           "SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"]
    missing_mysql = [var for var in required_mysql_vars if not os.getenv(var)]
    if missing_mysql:
        errors.append(f"Missing MySQL env vars: {missing_mysql}")
    
    if len(SF_OBJECT_FIELDS) < 5:
        errors.append("SF_OBJECT_FIELDS is incomplete! Add all Lead fields and types.")
    
    if errors:
        raise ValueError("Configuration errors:\n- " + "\n- ".join(errors))
    
    logger.info("✅ All configurations validated successfully")

def get_salesforce_token() -> Tuple[str, str]:
    """Fetch OAuth token from Salesforce."""
    auth_url = SF_CONFIGS["sf_auth_url"]
    payload = {
        "grant_type": "password",
        "client_id": SF_CONFIGS["sf_client_id"],
        "client_secret": SF_CONFIGS["sf_client_secret"],
        "username": SF_CONFIGS["sf_username"],
        "password": SF_CONFIGS["sf_password"]
    }
    
    try:
        logger.info("🔐 Authenticating with Salesforce...")
        response = requests.post(auth_url, data=payload, timeout=SF_CONFIGS["request_timeout"])
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
        sf = Salesforce(session_id=access_token, instance_url=instance_url)
        logger.info(f"✅ Connected to Salesforce (API version: {sf.sf_version})")
        return sf
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Salesforce: {e}")
        raise

def get_mysql_connection():
    """Create MySQL database connection."""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_CONFIGS["mysql_db_host"],
            port=MYSQL_CONFIGS["mysql_db_port"],
            user=MYSQL_CONFIGS["mysql_db_user"],
            password=MYSQL_CONFIGS["mysql_db_password"],
            database=MYSQL_CONFIGS["mysql_db_name"],
            autocommit=False
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE()")
            current_db = cursor.fetchone()[0]
            cursor.close()
            
            logger.info(f"✅ Connected to MySQL Server v{db_info}")
            logger.info(f"📂 Current database: {current_db}")
            logger.info(f"🎯 Target table: {current_db}.{MYSQL_CONFIGS['mysql_target_table_name']}")
            return connection
            
    except Error as e:
        logger.error(f"❌ Failed to connect to MySQL: {e}")
        raise

def get_fields_with_types() -> List[Dict[str, str]]:
    """Get field information from our static mapping."""
    fields_with_types = []
    
    for original_name, sf_type in SF_OBJECT_FIELDS.items():
        field_info = {
            'original_name': original_name,
            'normalized_name': normalize_field_name(original_name),
            'salesforce_type': sf_type,
            'mysql_type': map_salesforce_to_mysql_type(sf_type)
        }
        fields_with_types.append(field_info)
    
    logger.info(f"✅ Loaded {len(fields_with_types)} fields from static mapping")
    return fields_with_types

def create_mysql_table_if_not_exists(connection, fields_with_types: List[Dict[str, str]]):
    """Create MySQL table with proper data types."""
    table_name = MYSQL_CONFIGS["mysql_target_table_name"]
    
    cursor = connection.cursor()
    
    try:
        # Generate column definitions
        columns = []
        for field in fields_with_types:
            mysql_type = field['mysql_type']
            
            if mysql_type in ['TEXT']:
                column_def = f"`{field['normalized_name']}` {mysql_type}"
            elif mysql_type in ['DOUBLE', 'DECIMAL(18,2)', 'INT']:
                column_def = f"`{field['normalized_name']}` {mysql_type} DEFAULT NULL"
            elif mysql_type == 'TINYINT(1)':
                column_def = f"`{field['normalized_name']}` {mysql_type} DEFAULT 0"
            elif mysql_type in ['DATE', 'DATETIME']:
                column_def = f"`{field['normalized_name']}` {mysql_type} DEFAULT NULL"
            else:
                column_def = f"`{field['normalized_name']}` {mysql_type}"
            
            columns.append(column_def)
        
        # Add metadata columns
        columns.append("`_salesforce_id` VARCHAR(18) NOT NULL")
        
        # Create table with DYNAMIC row format for better TEXT handling
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)},
            PRIMARY KEY (`_salesforce_id`)
        ) ENGINE=InnoDB 
        ROW_FORMAT=DYNAMIC 
        DEFAULT CHARSET=utf8mb4 
        COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_table_sql)
        connection.commit()
        logger.info(f"✅ Table {table_name} ready with {len(columns)} columns")
        
    except Error as e:
        logger.error(f"❌ Failed to create table: {e}")
        raise
    finally:
        cursor.close()

def fetch_date_range(sf: Salesforce, soql_query: str, fields_with_types: List[Dict[str, str]], 
                     range_name: str, dry_run: bool = False) -> List[Dict[str, Any]]:
    """Fetch data for a specific date range using simple-salesforce Bulk API."""
    if dry_run:
        logger.info(f"🔍 DRY RUN - Would execute query for {range_name}")
        logger.info(f"   Query: {soql_query[:200]}...")
        return []
    
    range_results = []
    
    try:
        logger.info(f"📋 {range_name}: Executing bulk query...")
        
        bulk_object = getattr(sf.bulk, SF_CONFIGS['sf_object_name'])
        all_results = bulk_object.query(soql_query)
        
        for record in all_results:
            processed_record = {}
            for field in fields_with_types:
                orig_name = field['original_name']
                norm_name = field['normalized_name']
                value = record.get(orig_name, None)
                processed_record[norm_name] = value
            
            processed_record['_salesforce_id'] = record.get('Id', '')
            range_results.append(processed_record)
        
        logger.info(f"📦 {range_name}: Fetched {len(range_results):,} records")
        return range_results
        
    except Exception as e:
        logger.error(f"❌ {range_name}: Bulk query failed: {e}")
        logger.error(f"   SOQL: {soql_query[:200]}...")
        return []
def convert_value_for_mysql(value: Any, mysql_type: str, sf_type: str) -> Any:
    """Convert Salesforce values to MySQL-compatible values."""
    # Handle None/empty
    if value is None or value == '' or value == 'null':
        return None
    
    try:
        # Boolean
        if mysql_type.startswith('TINYINT'):
            if isinstance(value, bool):
                return 1 if value else 0
            return 1 if str(value).lower() in ('true', '1', 'yes') else 0
        
        # Numeric types
        elif mysql_type == 'DOUBLE':
            return float(value)
        elif mysql_type == 'INT':
            return int(float(value))
        elif mysql_type.startswith('DECIMAL'):
            return Decimal(str(value))
        
        # Date/DateTime - parse Salesforce ISO format or epoch ms
        elif mysql_type == 'DATE':
            if isinstance(value, str) and value:
                # Salesforce date format: YYYY-MM-DD
                return value.split('T')[0] if 'T' in value else value
            elif isinstance(value, (int, float)):
                try:
                    # Validate timestamp range before conversion
                    timestamp_sec = value / 1000.0
                    
                    # Windows timestamp limits (roughly 1970-01-01 to 3000-12-31)
                    # Unix systems are more permissive but we'll use conservative limits
                    MIN_TIMESTAMP = -2147483648  # ~1901
                    MAX_TIMESTAMP = 32503680000  # ~3000
                    
                    if not (MIN_TIMESTAMP <= timestamp_sec <= MAX_TIMESTAMP):
                        logger.warning(f"⚠️  Timestamp {value} out of valid range, returning None")
                        return None
                    
                    dt = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
                    return dt.strftime('%Y-%m-%d')
                except (ValueError, OverflowError, OSError) as e:
                    logger.warning(f"⚠️  Failed to convert epoch {value} to date: {e}")
                    return None
            return None
        
        elif mysql_type == 'DATETIME':
            if isinstance(value, str) and value:
                # Handle Salesforce datetime format: YYYY-MM-DDTHH:MM:SS.000Z or YYYY-MM-DDTHH:MM:SS.000+0000
                if 'T' in value:
                    try:
                        # Remove timezone and milliseconds, then convert to MySQL format
                        # Format: 2025-10-01T03:58:26.000Z -> 2025-10-01 03:58:26
                        datetime_part = value.split('.')[0]  # Remove milliseconds
                        datetime_part = datetime_part.replace('Z', '')  # Remove Z timezone
                        datetime_part = datetime_part.replace('T', ' ')  # Replace T with space
                        
                        # If there's still a timezone offset like +0000, remove it
                        if '+' in datetime_part:
                            datetime_part = datetime_part.split('+')[0]
                        
                        # Validate the resulting datetime format
                        datetime.strptime(datetime_part, '%Y-%m-%d %H:%M:%S')
                        
                        return datetime_part
                    except ValueError as e:
                        logger.warning(f"⚠️  Datetime parsing failed for '{value}': {e}")
                        
                        # Alternative approach: use string slicing
                        try:
                            # For format: 2025-10-01T03:58:26.000Z
                            if len(value) >= 19 and value[10] == 'T':
                                datetime_part = value[:19].replace('T', ' ')
                                datetime.strptime(datetime_part, '%Y-%m-%d %H:%M:%S')
                                return datetime_part
                        except ValueError:
                            logger.warning(f"⚠️  Alternative datetime parsing also failed for '{value}'")
                            return None
            elif isinstance(value, (int, float)):
                try:
                    # Validate timestamp range before conversion
                    timestamp_sec = value / 1000.0
                    
                    # Windows timestamp limits (roughly 1970-01-01 to 3000-12-31)
                    MIN_TIMESTAMP = -2147483648  # ~1901
                    MAX_TIMESTAMP = 32503680000  # ~3000
                    
                    if not (MIN_TIMESTAMP <= timestamp_sec <= MAX_TIMESTAMP):
                        logger.warning(f"⚠️  Timestamp {value} out of valid range, returning None")
                        return None
                    
                    dt = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, OverflowError, OSError) as e:
                    logger.warning(f"⚠️  Failed to convert epoch {value} to datetime: {e}")
                    return None
            return None
        
        # String types - handle None and convert to string
        else:
            return str(value) if value is not None else None
            
    except (ValueError, TypeError) as e:
        logger.warning(f"⚠️  Failed to convert value '{value}' to {mysql_type}: {e}")
        return None      

                    
def sync_range_to_mysql(connection, range_data: List[Dict[str, Any]], 
                        fields_with_types: List[Dict[str, str]], 
                        dry_run: bool = False, batch_size: int = 1000) -> int:
    """Sync a single date range to MySQL using batch inserts."""
    if dry_run:
        logger.info(f"🔍 DRY RUN - Would sync {len(range_data)} records")
        return 0
    
    if not range_data:
        return 0
    
    table_name = MYSQL_CONFIGS["mysql_target_table_name"]
    cursor = connection.cursor()
    
    try:
        # Prepare field lists
        normalized_fields = [field['normalized_name'] for field in fields_with_types]
        normalized_fields.append('_salesforce_id')
        
        # Create field type mapping
        field_type_map = {
            field['normalized_name']: (field['mysql_type'], field['salesforce_type']) 
            for field in fields_with_types
        }
        field_type_map['_salesforce_id'] = ('VARCHAR(18)', 'id')
        
        # Prepare INSERT statement with ON DUPLICATE KEY UPDATE
        placeholders = ', '.join(['%s'] * len(normalized_fields))
        columns = ', '.join([f"`{field}`" for field in normalized_fields])
        
        # For upsert: update all fields except primary key
        update_clause = ', '.join([
            f"`{field}` = VALUES(`{field}`)" 
            for field in normalized_fields if field != '_salesforce_id'
        ])
        
        insert_sql = f"""
            INSERT INTO `{table_name}` ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """
        
        # Process records in batches
        total_inserted = 0
        for i in range(0, len(range_data), batch_size):
            batch = range_data[i:i + batch_size]
            batch_values = []
            
            for record in batch:
                row_values = []
                for field in normalized_fields:
                    value = record.get(field)
                    mysql_type, sf_type = field_type_map.get(field, ('VARCHAR(255)', 'string'))
                    converted_value = convert_value_for_mysql(value, mysql_type, sf_type)
                    row_values.append(converted_value)
                batch_values.append(tuple(row_values))
            
            # Execute batch insert
            cursor.executemany(insert_sql, batch_values)
            connection.commit()
            total_inserted += len(batch)
            
            if (i + batch_size) % 5000 == 0:
                logger.info(f"   Progress: {total_inserted:,} / {len(range_data):,} records synced")
        
        logger.info(f"✅ Successfully synced {total_inserted:,} records to MySQL")
        return total_inserted
        
    except Error as e:
        connection.rollback()
        logger.error(f"❌ Failed to sync to MySQL: {e}")
        raise
    finally:
        cursor.close()

def verify_sync(connection, table_name: str):
    """Verify synced data in MySQL."""
    cursor = connection.cursor()
    
    try:
        # Get record count
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        
        # Get sample record
        cursor.execute(f"SELECT _salesforce_id, name, status, createddate FROM `{table_name}` LIMIT 1")
        sample = cursor.fetchone()
        
        logger.info(f"")
        logger.info(f"📊 Verification Results:")
        logger.info(f"   Total records in table: {count:,}")
        if sample:
            logger.info(f"   Sample record ID: {sample[0]}")
            logger.info(f"   Sample name: {sample[1]}")
        
        # Get table info
        cursor.execute(f"SHOW TABLE STATUS LIKE '{table_name}'")
        table_info = cursor.fetchone()
        if table_info:
            logger.info(f"   Table size: ~{table_info[6] / 1024 / 1024:.2f} MB")
        
    except Error as e:
        logger.warning(f"⚠️  Could not verify sync: {e}")
    finally:
        cursor.close()

def sync_date_range(sf, mysql_conn, fields_with_types: List[Dict[str, str]], 
                   range_name: str, start_date: str, end_date: str, 
                   dry_run: bool = False, batch_size: int = 1000) -> int:
    """Sync data for a specific date range."""
    try:
        if not dry_run and mysql_conn:
            create_mysql_table_if_not_exists(mysql_conn, fields_with_types)
        
        field_list = ", ".join([field['original_name'] for field in fields_with_types])
        
        logger.info(f"🔄 Processing {range_name}: {start_date.split('T')[0]} to {end_date.split('T')[0]}")
        
        soql_query = f"SELECT {field_list} FROM {SF_CONFIGS['sf_object_name']} WHERE CreatedDate >= {start_date} AND CreatedDate <= {end_date} ORDER BY CreatedDate"
        logger.info(f"📝 SOQL query length: {len(soql_query)} characters")
        
        range_results = fetch_date_range(sf, soql_query, fields_with_types, range_name, dry_run)
        
        if range_results and not dry_run and mysql_conn:
            range_synced = sync_range_to_mysql(mysql_conn, range_results, fields_with_types, dry_run, batch_size)
            logger.info(f"✅ {range_name}: Synced {range_synced:,} records")
            return range_synced
        elif range_results and dry_run:
            logger.info(f"🔍 DRY RUN - Found {len(range_results):,} records")
            return 0
        else:
            logger.info(f"📭 {range_name}: No records found")
            return 0
        
    except Exception as e:
        logger.error(f"💥 Sync failed for {range_name}: {e}")
        raise

def main():
    """Main function with CLI argument support."""
    mysql_conn = None
    
    try:
        args = parse_arguments()
        
        # Handle list-ranges option
        if args.list_ranges:
            list_available_ranges()
            return
        
        # Validate arguments
        if not args.date_range and not (args.start_date and args.end_date):
            logger.error("❌ Error: Must specify either --date-range or both --start-date and --end-date")
            logger.info("Run with --help for usage information")
            sys.exit(1)
        
        if args.date_range and (args.start_date or args.end_date):
            logger.error("❌ Error: Cannot use --date-range with --start-date/--end-date")
            sys.exit(1)
        
        logger.info("🚀 Starting Salesforce to MySQL (RDS) sync...")
        if args.dry_run:
            logger.info("🔍 DRY RUN MODE - No data will be synced")
        
        validate_configs()
        fields_with_types = get_fields_with_types()
        sf = get_sf_connection()
        
        if not args.dry_run:
            mysql_conn = get_mysql_connection()
        
        total_synced = 0
        
        # Handle predefined date range(s)
        if args.date_range:
            if args.date_range.upper() == 'ALL':
                logger.info(f"📅 Syncing ALL predefined ranges: {list(DATE_RANGES.keys())}")
                for range_name, range_data in DATE_RANGES.items():
                    synced = sync_date_range(
                        sf, mysql_conn, fields_with_types, range_name,
                        range_data['start'], range_data['end'],
                        args.dry_run, args.batch_size
                    )
                    total_synced += synced
            else:
                if args.date_range not in DATE_RANGES:
                    logger.error(f"❌ Error: Unknown date range '{args.date_range}'")
                    logger.info(f"Available ranges: {', '.join(DATE_RANGES.keys())}, ALL")
                    logger.info("Run with --list-ranges to see details")
                    sys.exit(1)
                
                range_data = DATE_RANGES[args.date_range]
                total_synced = sync_date_range(
                    sf, mysql_conn, fields_with_types, args.date_range,
                    range_data['start'], range_data['end'],
                    args.dry_run, args.batch_size
                )
        
        # Handle custom date range
        else:
            start_date = parse_custom_date(args.start_date)
            end_date = parse_custom_date(args.end_date)
            
            if start_date > end_date:
                logger.error("❌ Error: Start date must be before end date")
                sys.exit(1)
            
            range_name = f"CUSTOM_{args.start_date}_to_{args.end_date}"
            total_synced = sync_date_range(
                sf, mysql_conn, fields_with_types, range_name,
                start_date, end_date,
                args.dry_run, args.batch_size
            )
        
        if args.dry_run:
            logger.info("🔍 DRY RUN completed - No data was synced")
        else:
            logger.info(f"🎉 Sync completed successfully! Total records synced: {total_synced:,}")
            
            # Verify the sync
            if mysql_conn and total_synced > 0:
                verify_sync(mysql_conn, MYSQL_CONFIGS["mysql_target_table_name"])
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Sync interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Sync failed: {e}")
        raise
    finally:
        # Close MySQL connection
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
            logger.info("📪 MySQL connection closed")

if __name__ == "__main__":
    main()