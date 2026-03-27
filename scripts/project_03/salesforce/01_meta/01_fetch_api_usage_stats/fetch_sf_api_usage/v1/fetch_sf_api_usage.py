import requests
import os
import csv
import json
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

class SalesforceAPIMonitor:
    def __init__(self):
        self.sf_client_id = os.getenv("sf_client_id")
        self.sf_client_secret = os.getenv("sf_client_secret")
        self.sf_username = os.getenv("sf_username")
        self.sf_password = os.getenv("sf_password") + os.getenv("sf_security_token", "")
        self.SF_AUTH_URL = "https://login.salesforce.com/services/oauth2/token"
        self.access_token = None
        self.instance_url = None

    def get_salesforce_token(self):
        """Fetch OAuth token from Salesforce."""
        payload = {
            "grant_type": "password",
            "client_id": self.sf_client_id,
            "client_secret": self.sf_client_secret,
            "username": self.sf_username,
            "password": self.sf_password,
        }
        
        try:
            response = requests.post(self.SF_AUTH_URL, data=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            self.access_token = data.get("access_token")
            self.instance_url = data.get("instance_url", "").rstrip("/")
            
            print("✅ Salesforce authentication successful!")
            print(f"📋 Instance URL: {self.instance_url}")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Authentication failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"📄 Response details: {e.response.text}")
            return False

    def get_latest_api_version(self):
        """Dynamically get the latest API version."""
        if not self.access_token or not self.instance_url:
            print("❌ Not authenticated. Please call get_salesforce_token() first.")
            return None
            
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        try:
            response = requests.get(
                f"{self.instance_url}/services/data/", 
                headers=headers, 
                timeout=30
            )
            response.raise_for_status()
            
            versions = response.json()
            latest_version = versions[-1]['version']
            print(f"🔗 Using Salesforce API version: {latest_version}")
            return latest_version
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch API versions: {e}")
            return "58.0"  # Fallback to default version

    def get_api_usage(self, api_version=None):
        """Fetch API usage limits from Salesforce."""
        if not self.access_token or not self.instance_url:
            print("❌ Not authenticated. Please call get_salesforce_token() first.")
            return None
        
        if not api_version:
            api_version = self.get_latest_api_version()
            
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.instance_url}/services/data/v{api_version}/limits"
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to fetch API usage: {e}")
            return None

    def format_resource_name(self, resource_name):
        """Convert camelCase or lowercase resource names to readable format."""
        # Add spaces before capital letters for camelCase
        formatted = ''.join([' ' + char if char.isupper() else char for char in resource_name])
        # Replace common abbreviations
        replacements = {
            'api': 'API',
            'mb': 'MB',
            'cdp': 'CDP',
            'ai': 'AI',
            'einstein': 'Einstein',
            'crm': 'CRM',
            'id': 'ID',
            'odata': 'OData',
            'v2': 'v2'
        }
        
        words = formatted.split()
        formatted_words = []
        for word in words:
            if word.lower() in replacements:
                formatted_words.append(replacements[word.lower()])
            else:
                formatted_words.append(word.title())
        
        return ' '.join(formatted_words)

    def format_usage_data(self, usage_data):
        """Format and display API usage data in a readable way."""
        if not usage_data:
            return
            
        print("\n" + "="*70)
        print("📊 SALESFORCE API USAGE LIMITS")
        print("="*70)
        
        # Filter out resources with zero usage or only show important ones
        important_resources = [
            'DailyApiRequests', 'DataStorageMB', 'FileStorageMB', 
            'DailyBulkV2QueryJobs', 'DailyAsyncApexExecutions',
            'PermissionSets', 'ExternalServicesRegistrations'
        ]
        
        displayed_count = 0
        for resource, limits in sorted(usage_data.items()):
            if isinstance(limits, dict) and 'Max' in limits and 'Remaining' in limits:
                max_limit = limits['Max']
                remaining = limits['Remaining']
                used = max_limit - remaining
                
                # Skip if both used and max are 0, or if usage is negligible for non-important resources
                if max_limit == 0:
                    continue
                    
                usage_percentage = (used / max_limit * 100) if max_limit > 0 else 0
                
                # Only show resources with significant usage OR important resources
                if (usage_percentage > 5 or 
                    resource in important_resources or 
                    'Daily' in resource or 
                    'Storage' in resource):
                    
                    formatted_name = self.format_resource_name(resource)
                    
                    print(f"\n🔹 {formatted_name}:")
                    print(f"   📈 Used: {used:,} / {max_limit:,} ({usage_percentage:.1f}%)")
                    print(f"   💾 Remaining: {remaining:,}")
                    
                    # Add visual progress bar with zero division protection
                    bar_length = 20
                    if max_limit > 0:
                        filled_length = int(bar_length * used / max_limit)
                    else:
                        filled_length = 0
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)
                    print(f"   📊 Progress: [{bar}] {usage_percentage:.1f}%")
                    
                    displayed_count += 1
                    
                    # Limit display to prevent overwhelming output
                    if displayed_count >= 25:
                        remaining_resources = len([r for r in usage_data.keys() if isinstance(usage_data[r], dict)]) - displayed_count
                        if remaining_resources > 0:
                            print(f"\n📋 ... and {remaining_resources} more resources with minimal usage")
                        break

    def save_usage_to_csv(self, usage_data, filename_prefix="salesforce_api_usage"):
        """Save API usage data to a CSV file with timestamp."""
        if not usage_data:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'timestamp', 'resource_name', 'formatted_name', 'max_limit', 
                    'remaining', 'used', 'usage_percentage',
                    'instance_url', 'api_version'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                current_time = datetime.now().isoformat()
                api_version = self.get_latest_api_version()
                
                for resource, limits in sorted(usage_data.items()):
                    if isinstance(limits, dict) and 'Max' in limits and 'Remaining' in limits:
                        max_limit = limits['Max']
                        remaining = limits['Remaining']
                        used = max_limit - remaining
                        
                        # Skip resources with max_limit = 0 to avoid division issues
                        if max_limit == 0:
                            continue
                            
                        usage_percentage = (used / max_limit * 100) if max_limit > 0 else 0
                        
                        writer.writerow({
                            'timestamp': current_time,
                            'resource_name': resource,
                            'formatted_name': self.format_resource_name(resource),
                            'max_limit': max_limit,
                            'remaining': remaining,
                            'used': used,
                            'usage_percentage': round(usage_percentage, 2),
                            'instance_url': self.instance_url,
                            'api_version': api_version
                        })
                    elif isinstance(limits, (int, float)):
                        writer.writerow({
                            'timestamp': current_time,
                            'resource_name': resource,
                            'formatted_name': self.format_resource_name(resource),
                            'max_limit': limits,
                            'remaining': 'N/A',
                            'used': 'N/A',
                            'usage_percentage': 'N/A',
                            'instance_url': self.instance_url,
                            'api_version': api_version
                        })
                
            print(f"💾 API usage data saved to CSV: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error saving to CSV: {e}")
            return None

    def save_usage_to_json(self, usage_data, filename_prefix="salesforce_api_usage"):
        """Save API usage data to a JSON file with timestamp (for backup)."""
        if not usage_data:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.json"
        
        try:
            data_to_save = {
                "timestamp": datetime.now().isoformat(),
                "instance_url": self.instance_url,
                "api_version": self.get_latest_api_version(),
                "api_usage": usage_data
            }
            
            with open(filename, 'w') as f:
                json.dump(data_to_save, f, indent=2)
                
            print(f"📁 JSON backup saved to: {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error saving to JSON: {e}")
            return None

    def check_critical_limits(self, usage_data, threshold=20):
        """Check if any API limits are approaching exhaustion."""
        if not usage_data:
            return []
            
        print(f"\n🚨 CRITICAL LIMITS CHECK (Below {threshold}% remaining):")
        print("-" * 50)
        
        critical_resources = []
        warning_resources = []
        
        for resource, limits in usage_data.items():
            if isinstance(limits, dict) and 'Max' in limits and 'Remaining' in limits:
                max_limit = limits['Max']
                remaining = limits['Remaining']
                
                # Skip resources with max_limit = 0
                if max_limit <= 0:
                    continue
                    
                remaining_percentage = (remaining / max_limit) * 100
                
                if remaining_percentage < 10:
                    critical_resources.append((resource, remaining_percentage, remaining, max_limit))
                elif remaining_percentage < threshold:
                    warning_resources.append((resource, remaining_percentage, remaining, max_limit))
        
        # Print critical resources first
        for resource, percentage, remaining, max_limit in critical_resources:
            formatted_name = self.format_resource_name(resource)
            print(f"🔴 CRITICAL {formatted_name}: {percentage:.1f}% remaining ({remaining:,}/{max_limit:,})")
        
        # Print warning resources
        for resource, percentage, remaining, max_limit in warning_resources:
            formatted_name = self.format_resource_name(resource)
            print(f"🟡 WARNING {formatted_name}: {percentage:.1f}% remaining ({remaining:,}/{max_limit:,})")
        
        if not critical_resources and not warning_resources:
            print("✅ All limits are within safe thresholds")
        
        return critical_resources + warning_resources

    def generate_summary_report(self, usage_data):
        """Generate a summary report of key metrics."""
        if not usage_data:
            return
            
        print(f"\n📈 KEY METRICS SUMMARY")
        print("-" * 40)
        
        key_metrics = {
            'DailyApiRequests': 'Daily API Requests',
            'DataStorageMB': 'Data Storage (MB)',
            'FileStorageMB': 'File Storage (MB)',
            'DailyBulkV2QueryJobs': 'Bulk API v2 Jobs',
            'DailyAsyncApexExecutions': 'Async Apex Executions',
            'PermissionSets': 'Permission Sets'
        }
        
        for metric_key, metric_name in key_metrics.items():
            if metric_key in usage_data:
                limits = usage_data[metric_key]
                if isinstance(limits, dict) and 'Max' in limits and 'Remaining' in limits:
                    max_limit = limits['Max']
                    remaining = limits['Remaining']
                    used = max_limit - remaining
                    if max_limit > 0:
                        percentage = (used / max_limit * 100)
                        print(f"  {metric_name}: {used:,}/{max_limit:,} ({percentage:.1f}%)")

def main():
    """Main function to authenticate and monitor Salesforce API usage."""
    print("🚀 Salesforce API Usage Monitor")
    print("=" * 40)
    
    # Initialize the monitor
    monitor = SalesforceAPIMonitor()
    
    # Authenticate
    if not monitor.get_salesforce_token():
        print("❌ Authentication failed. Exiting.")
        exit(1)
    
    # Get API usage data
    api_usage = monitor.get_api_usage()
    
    if api_usage:
        # Generate summary first
        monitor.generate_summary_report(api_usage)
        
        # Display formatted usage data
        monitor.format_usage_data(api_usage)
        
        # Check for critical limits
        critical_limits = monitor.check_critical_limits(api_usage)
        
        # Save to CSV (primary format)
        csv_file = monitor.save_usage_to_csv(api_usage)
        
        # Optional: Save to JSON as backup
        json_file = monitor.save_usage_to_json(api_usage)
        
        print(f"\n✅ API usage monitoring completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if critical_limits:
            print(f"⚠️  Found {len(critical_limits)} resources needing attention")
        
        if csv_file:
            print(f"📊 Data ready for analysis in: {csv_file}")
    else:
        print("❌ Failed to retrieve API usage data.")

if __name__ == "__main__":
    main()