from dotenv import load_dotenv
import os 
import requests
import csv
import logging
import sys
from typing import Tuple, List, Dict, Any
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
REQUEST_TIMEOUT = 30
SF_AUTH_URL = "https://login.salesforce.com/services/oauth2/token"
SF_API_VERSION = "58.0"

# Load environment variables
load_dotenv()

# Retrieve credentials
sf_client_id = os.getenv("sf_client_id")
sf_client_secret = os.getenv("sf_client_secret")
sf_username = os.getenv("sf_username")
sf_password = os.getenv("sf_password") + os.getenv("sf_security_token", "")


def validate_config():
    """Validate that all required environment variables are present."""
    required_vars = ["sf_client_id", "sf_client_secret", "sf_username", "sf_password"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        raise ValueError(f"Missing required environment variables: {missing}")


def get_salesforce_token() -> Tuple[str, str]:
    """Fetch OAuth token from Salesforce with enhanced error handling."""
    payload = {
        "grant_type": "password",
        "client_id": sf_client_id,
        "client_secret": sf_client_secret,
        "username": sf_username,
        "password": sf_password
    }
    
    try:
        logger.info("🔐 Authenticating with Salesforce...")
        response = requests.post(SF_AUTH_URL, data=payload, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        access_token = data["access_token"]
        instance_url = data["instance_url"].strip("/")
        
        logger.info("✅ Successfully authenticated with Salesforce!")
        return access_token, instance_url
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Authentication failed: {e}")
        raise


def fetch_salesforce_object_fields(access_token: str, instance_url: str, 
                                 object_name: str) -> List[Dict[str, Any]]:
    """
    Fetch all fields for a specified Salesforce object.
    
    Args:
        access_token: Salesforce OAuth access token
        instance_url: Salesforce instance URL
        object_name: Salesforce object name (e.g., 'Lead', 'Account', 'Contact')
    
    Returns:
        List of field metadata dictionaries
    """
    describe_url = f"{instance_url}/services/data/v{SF_API_VERSION}/sobjects/{object_name}/describe"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        logger.info(f"🔍 Fetching field metadata for {object_name} object...")
        response = requests.get(describe_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        fields = data.get("fields", [])
        
        # Extract relevant field information
        field_metadata = []
        for field in fields:
            field_info = {
                "field_name": field.get("name", ""),
                "label": field.get("label", ""),
                "type": field.get("type", ""),
                "length": field.get("length", ""),
                "precision": field.get("precision", ""),
                "scale": field.get("scale", ""),
                "default_value": field.get("defaultValue", ""),
                "required": field.get("nillable", True) is False or field.get("createable", False) and field.get("nillable", True) is False,
                "unique": field.get("unique", False),
                "external_id": field.get("externalId", False),
                "calculated": field.get("calculated", False),
                "calculated_formula": field.get("calculatedFormula", ""),
                "reference_to": ", ".join(field.get("referenceTo", [])),
                "relationship_name": field.get("relationshipName", ""),
                "compound_field_name": field.get("compoundFieldName", ""),
                "filterable": field.get("filterable", False),
                "sortable": field.get("sortable", False),
                "createable": field.get("createable", False),
                "updateable": field.get("updateable", False),
                "auto_number": field.get("autoNumber", False),
                "restricted_picklist": field.get("restrictedPicklist", False),
                "picklist_values": len(field.get("picklistValues", [])),
                "description": field.get("inlineHelpText", "")
            }
            field_metadata.append(field_info)
        
        logger.info(f"✅ Retrieved {len(field_metadata)} fields for {object_name} object")
        return field_metadata
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Failed to fetch fields for {object_name}: {e}")
        raise


def save_to_csv(field_metadata: List[Dict[str, Any]], object_name: str) -> str:
    """
    Save field metadata to a CSV file with timestamp in filename.
    
    Args:
        field_metadata: List of field metadata dictionaries
        object_name: Salesforce object name (used for filename)
    
    Returns:
        Filename of the created CSV
    """
    if not field_metadata:
        logger.warning("No field metadata to save")
        return ""
    
    # Generate timestamp in YYYYMMDD_HHMMSS format
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{object_name}_fields_metadata_{timestamp}.csv"
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Define CSV columns
            fieldnames = [
                "field_name", "label", "type", "length", "precision", "scale",
                "default_value", "required", "unique", "external_id", "calculated",
                "calculated_formula", "reference_to", "relationship_name",
                "compound_field_name", "filterable", "sortable", "createable",
                "updateable", "auto_number", "restricted_picklist", 
                "picklist_values", "description"
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(field_metadata)
        
        logger.info(f"💾 Successfully saved {len(field_metadata)} fields to {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"❌ Failed to save field metadata to CSV: {e}")
        raise


def print_usage():
    """Print usage instructions."""
    print("Usage: python salesforce_fields_fetcher.py <object_name>")
    print("\nExamples:")
    print("  python salesforce_fields_fetcher.py Lead")
    print("  python salesforce_fields_fetcher.py Account")
    print("  python salesforce_fields_fetcher.py Contact")
    print("  python salesforce_fields_fetcher.py Opportunity")
    print("\nCommon Salesforce objects:")
    print("  Lead, Account, Contact, Opportunity, Case, Campaign, User, Product2")


def main():
    """Main function to fetch and save Salesforce object fields."""
    try:
        # Check command line arguments
        if len(sys.argv) != 2:
            print_usage()
            return 1
        
        object_name = sys.argv[1].strip()
        
        if not object_name:
            logger.error("❌ Object name cannot be empty")
            print_usage()
            return 1
        
        # Validate configuration
        validate_config()
        
        # Get Salesforce access
        access_token, instance_url = get_salesforce_token()
        logger.info("✅ Salesforce connection established successfully!")
        
        logger.info(f"📋 Processing object: {object_name}")
        
        # Fetch field metadata
        field_metadata = fetch_salesforce_object_fields(
            access_token, instance_url, object_name
        )
        
        # Save to CSV
        filename = save_to_csv(field_metadata, object_name)
        
        # Display summary
        logger.info("📊 Field Metadata Summary:")
        logger.info(f"   - Total fields: {len(field_metadata)}")
        
        # Count field types
        field_types = {}
        for field in field_metadata:
            field_type = field['type']
            field_types[field_type] = field_types.get(field_type, 0) + 1
        
        for field_type, count in sorted(field_types.items()):
            logger.info(f"   - {field_type}: {count}")
        
        logger.info(f"✅ Script completed! Field metadata saved to: {filename}")
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())