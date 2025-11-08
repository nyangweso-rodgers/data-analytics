import os
import logging
from dotenv import load_dotenv
import requests
from datetime import datetime
import csv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_salesforce_token() -> str:
    """Get Salesforce access token using REST API."""
    auth_url = "https://login.salesforce.com/services/oauth2/token"
    
    payload = {
        "grant_type": "password",
        "client_id": os.getenv("sf_client_id"),
        "client_secret": os.getenv("sf_client_secret"),
        "username": os.getenv("sf_username"),
        "password": os.getenv("sf_password") + os.getenv("sf_security_token", "")
    }
    
    try:
        logger.info("🔐 Authenticating with Salesforce...")
        response = requests.post(auth_url, data=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        access_token = data["access_token"]
        instance_url = data["instance_url"]
        
        logger.info("✅ Successfully authenticated with Salesforce!")
        return access_token, instance_url
        
    except Exception as e:
        logger.error(f"❌ Authentication failed: {e}")
        raise

def get_lead_date_range(access_token: str, instance_url: str) -> tuple:
    """Get min and max CreatedDate using REST API."""
    try:
        logger.info("📅 Getting Lead date range...")
        
        query = "SELECT MIN(CreatedDate), MAX(CreatedDate) FROM Lead"
        url = f"{instance_url}/services/data/v58.0/query?q={query}"
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        min_date = data['records'][0]['expr0']
        max_date = data['records'][0]['expr1']
        
        logger.info(f"✅ Date range: {min_date} to {max_date}")
        return min_date, max_date
        
    except Exception as e:
        logger.error(f"❌ Failed to get date range: {e}")
        raise

def get_lead_count_by_year_individual(access_token: str, instance_url: str, start_year: int, end_year: int) -> dict:
    """Get lead count for each year individually."""
    year_counts = {}
    
    for year in range(start_year, end_year + 1):
        try:
            logger.info(f"📊 Counting leads for {year}...")
            
            query = f"""
            SELECT COUNT(Id) 
            FROM Lead 
            WHERE CreatedDate >= {year}-01-01T00:00:00Z 
            AND CreatedDate <= {year}-12-31T23:59:59Z
            """
            url = f"{instance_url}/services/data/v58.0/query?q={query}"
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            count = data['records'][0]['expr0']
            year_counts[year] = count
            
            logger.info(f"  {year}: {count:,} leads")
            
        except Exception as e:
            logger.error(f"❌ Failed to count {year}: {e}")
            year_counts[year] = 0
    
    return year_counts

def generate_date_ranges(year_counts: dict) -> dict:
    """Generate date ranges based on year counts."""
    ranges = {}
    
    for year, count in year_counts.items():
        if count > 100000:  # Split heavy years into quarters
            quarters = [
                (f"{year}-01-01T00:00:00Z", f"{year}-03-31T23:59:59Z", f"{year}_Q1"),
                (f"{year}-04-01T00:00:00Z", f"{year}-06-30T23:59:59Z", f"{year}_Q2"),
                (f"{year}-07-01T00:00:00Z", f"{year}-09-30T23:59:59Z", f"{year}_Q3"),
                (f"{year}-10-01T00:00:00Z", f"{year}-12-31T23:59:59Z", f"{year}_Q4"),
            ]
            for start, end, name in quarters:
                ranges[name] = {
                    'start': start,
                    'end': end,
                    'estimated_count': count // 4
                }
        else:  # Use full year
            ranges[f"Year_{year}"] = {
                'start': f"{year}-01-01T00:00:00Z",
                'end': f"{year}-12-31T23:59:59Z", 
                'estimated_count': count
            }
    
    return ranges

def write_report(year_counts: dict, date_ranges: dict, min_date: str, max_date: str):
    """Write analysis report to CSV."""
    filename = f"lead_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Header
        writer.writerow(['Lead Distribution Analysis'])
        writer.writerow(['Generated', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Date Range', f"{min_date} to {max_date}"])
        writer.writerow(['Total Leads', sum(year_counts.values())])
        writer.writerow([])
        
        # Year counts
        writer.writerow(['Year', 'Lead Count'])
        for year, count in sorted(year_counts.items()):
            writer.writerow([year, count])
        
        writer.writerow([])
        writer.writerow(['Suggested Date Ranges for Sync'])
        writer.writerow(['Range Name', 'Start Date', 'End Date', 'Estimated Count'])
        
        for name, range_data in sorted(date_ranges.items()):
            writer.writerow([
                name,
                range_data['start'],
                range_data['end'], 
                range_data['estimated_count']
            ])
    
    logger.info(f"📄 Report written to: {filename}")
    return filename

def print_static_ranges(date_ranges: dict):
    """Print static ranges for main script."""
    logger.info("📋 STATIC RANGES for main script:")
    print("\n# Copy this into your main script:\n")
    print("DATE_RANGES = {")
    for name, range_data in sorted(date_ranges.items()):
        print(f"    '{name}': {{")
        print(f"        'start': '{range_data['start']}',")
        print(f"        'end': '{range_data['end']}',")
        print(f"        'estimated_count': {range_data['estimated_count']}")
        print("    },")
    print("}")

def main():
    """Main analysis function."""
    try:
        logger.info("🔍 Starting Lead distribution analysis...")
        
        # Get access token
        access_token, instance_url = get_salesforce_token()
        
        # Get date range
        min_date, max_date = get_lead_date_range(access_token, instance_url)
        
        # Extract years from date range
        start_year = int(min_date[:4])
        end_year = int(max_date[:4])
        
        logger.info(f"📅 Analyzing years {start_year} to {end_year}")
        
        # Get counts by year (individual queries)
        year_counts = get_lead_count_by_year_individual(access_token, instance_url, start_year, end_year)
        
        # Print summary
        logger.info("📈 LEAD COUNTS BY YEAR:")
        total_leads = 0
        for year, count in sorted(year_counts.items()):
            logger.info(f"  {year}: {count:,} leads")
            total_leads += count
        logger.info(f"  TOTAL: {total_leads:,} leads")
        
        # Generate date ranges
        date_ranges = generate_date_ranges(year_counts)
        
        # Write CSV report
        report_file = write_report(year_counts, date_ranges, min_date, max_date)
        
        # Print static ranges
        print_static_ranges(date_ranges)
        
        logger.info(f"🎉 Analysis complete! Check {report_file}")
        
    except Exception as e:
        logger.error(f"💥 Analysis failed: {e}")

if __name__ == "__main__":
    main()