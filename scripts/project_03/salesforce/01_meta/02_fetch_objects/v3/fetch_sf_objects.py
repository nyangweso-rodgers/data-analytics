from dotenv import load_dotenv
import os
import requests
from datetime import datetime
import pandas as pd
from simple_salesforce import Salesforce
import argparse

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
        instance_url = data.get("instance_url")
        print("✅ Successfully authenticated with Salesforce!")
        return access_token, instance_url
    else:
        print(f"❌ Authentication failed: {response.status_code} {response.text}")
        return None, None

def get_record_count(sf, object_name):
    """Get record count for a specific object."""
    try:
        query = f"SELECT COUNT() FROM {object_name}"
        result = sf.query(query)
        return result.get("totalSize", 0)
    except Exception as e:
        # Silently return -1 for objects that can't be queried
        # This includes metadata objects, system objects, and objects with restrictions
        return -1

def fetch_all_objects_with_counts(access_token, instance_url, sf):
    """Fetch all Salesforce objects with their metadata and record counts."""
    
    try:
        # Get API version
        versions_response = requests.get(
            f"{instance_url}/services/data/",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if versions_response.status_code != 200:
            print("❌ Failed to get API versions")
            return None, None
            
        versions = versions_response.json()
        latest_version = versions[-1]['version']
        print(f"📊 Using Salesforce API version: {latest_version}")
        
        # Get all objects
        response = requests.get(
            f"{instance_url}/services/data/v{latest_version}/sobjects/",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        
        if response.status_code != 200:
            print(f"❌ Failed to fetch objects: {response.text}")
            return None, None
        
        objects_data = response.json()
        objects = objects_data.get("sobjects", [])
        
        print(f"\n📌 Found {len(objects)} total objects")
        print(f"⏳ Fetching record counts (this may take a few minutes)...\n")
        
        # Prepare data for Excel
        all_objects_data = []
        custom_count = 0
        standard_count = 0
        total_records = 0
        processed = 0
        error_count = 0
        
        for obj in objects:
            processed += 1
            object_name = obj['name']
            is_custom = obj.get('custom', False)
            is_queryable = obj.get('queryable', False)
            
            # Only try to get count if object is queryable
            if is_queryable:
                print(f"Processing {processed}/{len(objects)}: {object_name}...", end="\r")
                record_count = get_record_count(sf, object_name)
                
                # Track errors but don't stop processing
                if record_count == -1:
                    error_count += 1
            else:
                record_count = 0  # Not queryable
            
            # Track statistics
            if is_custom:
                custom_count += 1
            else:
                standard_count += 1
            
            if record_count > 0:
                total_records += record_count
            
            obj_info = {
                'Object Name': object_name,
                'Label': obj.get('label', ''),
                'Custom Object': 'Yes' if is_custom else 'No',
                'Record Count': record_count if record_count >= 0 else 'N/A',
                'Queryable': 'Yes' if is_queryable else 'No',
                'Creatable': 'Yes' if obj.get('createable', False) else 'No',
                'Updateable': 'Yes' if obj.get('updateable', False) else 'No',
                'Deletable': 'Yes' if obj.get('deletable', False) else 'No',
            }
            all_objects_data.append(obj_info)
        
        print("\n✅ Finished processing all objects")
        if error_count > 0:
            print(f"ℹ️  Note: {error_count} objects could not be queried (this is normal for system/metadata objects)")
        
        # Create summary data
        summary_data = [{
            'Metric': 'Total Objects',
            'Value': len(objects)
        }, {
            'Metric': 'Standard Objects',
            'Value': standard_count
        }, {
            'Metric': 'Custom Objects',
            'Value': custom_count
        }, {
            'Metric': 'Objects with Query Errors',
            'Value': error_count
        }, {
            'Metric': 'Total Records (Queryable Objects)',
            'Value': f"{total_records:,}"
        }, {
            'Metric': 'API Version',
            'Value': latest_version
        }, {
            'Metric': 'Instance URL',
            'Value': instance_url
        }, {
            'Metric': 'Extraction Date',
            'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }]
        
        return all_objects_data, summary_data
        
    except Exception as e:
        print(f"❌ Error fetching objects: {e}")
        return None, None

def fetch_specific_objects(access_token, instance_url, sf, object_names, latest_version):
    """Fetch specific Salesforce objects with their metadata and record counts."""
    
    print(f"\n📌 Fetching data for {len(object_names)} specified object(s)")
    print(f"⏳ Processing objects...\n")
    
    all_objects_data = []
    custom_count = 0
    standard_count = 0
    total_records = 0
    not_found = []
    
    for idx, object_name in enumerate(object_names, 1):
        print(f"Processing {idx}/{len(object_names)}: {object_name}...")
        
        # Get object metadata
        try:
            response = requests.get(
                f"{instance_url}/services/data/v{latest_version}/sobjects/{object_name}/describe/",
                headers={"Authorization": f"Bearer {access_token}"}
            )
            
            if response.status_code != 200:
                print(f"  ❌ Object '{object_name}' not found or not accessible")
                not_found.append(object_name)
                continue
                
            metadata = response.json()
        except Exception as e:
            print(f"  ❌ Error fetching metadata for {object_name}: {e}")
            not_found.append(object_name)
            continue
        
        is_custom = metadata.get('custom', False)
        is_queryable = metadata.get('queryable', False)
        
        # Get record count if queryable
        if is_queryable:
            record_count = get_record_count(sf, object_name)
        else:
            record_count = 0
            print(f"  ℹ️  Object is not queryable")
        
        # Track statistics
        if is_custom:
            custom_count += 1
        else:
            standard_count += 1
        
        if record_count > 0:
            total_records += record_count
        
        obj_info = {
            'Object Name': object_name,
            'Label': metadata.get('label', ''),
            'Custom Object': 'Yes' if is_custom else 'No',
            'Record Count': record_count if record_count >= 0 else 'N/A',
            'Queryable': 'Yes' if is_queryable else 'No',
            'Creatable': 'Yes' if metadata.get('createable', False) else 'No',
            'Updateable': 'Yes' if metadata.get('updateable', False) else 'No',
            'Deletable': 'Yes' if metadata.get('deletable', False) else 'No',
        }
        all_objects_data.append(obj_info)
        
        if record_count >= 0:
            print(f"  ✅ Found {record_count:,} records")
    
    print("\n✅ Finished processing specified objects")
    if not_found:
        print(f"⚠️  Could not find: {', '.join(not_found)}")
    
    # Create summary data
    summary_data = [{
        'Metric': 'Total Objects Requested',
        'Value': len(object_names)
    }, {
        'Metric': 'Objects Found',
        'Value': len(all_objects_data)
    }, {
        'Metric': 'Objects Not Found',
        'Value': len(not_found)
    }, {
        'Metric': 'Standard Objects',
        'Value': standard_count
    }, {
        'Metric': 'Custom Objects',
        'Value': custom_count
    }, {
        'Metric': 'Total Records',
        'Value': f"{total_records:,}"
    }, {
        'Metric': 'Instance URL',
        'Value': instance_url
    }, {
        'Metric': 'Extraction Date',
        'Value': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }]
    
    return all_objects_data, summary_data

def save_to_excel(objects_data, summary_data, filename_prefix="salesforce_objects_report"):
    """Save data to Excel file with two sheets."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.xlsx"
    
    try:
        # Create DataFrames
        df_objects = pd.DataFrame(objects_data)
        df_summary = pd.DataFrame(summary_data)
        
        # Sort objects by record count (descending)
        df_objects['Sort_Count'] = pd.to_numeric(
            df_objects['Record Count'].replace('N/A', -1), 
            errors='coerce'
        )
        df_objects = df_objects.sort_values('Sort_Count', ascending=False)
        df_objects = df_objects.drop('Sort_Count', axis=1)
        
        # Write to Excel with multiple sheets
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df_objects.to_excel(writer, sheet_name='All Objects', index=False)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # Auto-adjust column widths
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n✅ Excel report successfully saved to: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Error saving to Excel: {e}")
        return None

def main():
    """Main execution function."""
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Fetch Salesforce object metadata and record counts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch all objects
  python fetch_sf_objects.py --all
  
  # Fetch specific objects
  python fetch_sf_objects.py --objects Account Contact Lead
  
  # Fetch custom objects with specific names
  python fetch_sf_objects.py --objects Agent__c Property__c
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true',
                      help='Fetch all Salesforce objects')
    group.add_argument('--objects', nargs='+', metavar='OBJECT',
                      help='Fetch specific objects (space-separated list)')
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Salesforce object extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Get Salesforce access token and instance URL
    access_token, instance_url = get_salesforce_token()
    
    if not access_token or not instance_url:
        print("Failed to authenticate with Salesforce.")
        return
    
    # Initialize Salesforce connection
    sf = Salesforce(instance_url=instance_url, session_id=access_token)
    
    # Get API version
    try:
        versions_response = requests.get(
            f"{instance_url}/services/data/",
            headers={"Authorization": f"Bearer {access_token}"}
        )
        versions = versions_response.json()
        latest_version = versions[-1]['version']
    except Exception as e:
        print(f"❌ Error getting API version: {e}")
        return
    
    # Fetch objects based on arguments
    if args.all:
        print("📋 Mode: Fetching ALL objects")
        objects_data, summary_data = fetch_all_objects_with_counts(access_token, instance_url, sf)
        filename_prefix = "salesforce_all_objects_report"
    else:  # args.objects
        print(f"📋 Mode: Fetching SPECIFIC objects: {', '.join(args.objects)}")
        objects_data, summary_data = fetch_specific_objects(access_token, instance_url, sf, args.objects, latest_version)
        filename_prefix = "salesforce_specific_objects_report"
    
    if objects_data and summary_data:
        # Save to Excel
        save_to_excel(objects_data, summary_data, filename_prefix)
        print(f"\n✅ Extraction completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("❌ Failed to fetch object data")

if __name__ == "__main__":
    main()