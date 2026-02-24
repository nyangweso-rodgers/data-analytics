import os
import sys
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from clickhouse_driver import Client
import requests

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# CLICKHOUSE CONFIGURATION
# ============================================================================
CLICKHOUSE_CONFIG = {
    "host": os.getenv("SC_CH_DB_HOST"),
    "port": int(os.getenv("SC_CH_DB_PORT", "8443")),
    "username": os.getenv("SC_CH_DB_USER"),
    "password": os.getenv("SC_CH_DB_PASSWORD"),
    "database": "kobo_collect",  # TODO: Update if different
    "secure": True,
    "verify": True,
}

KOBO_API_TOKEN = os.getenv("SC_KOBOTOOLBOX_API_TOKEN")

# ============================================================================
# FORM CONFIGURATION
# Define the form to sync
# ============================================================================
FORM_CONFIG = {
    "name": "Carbon 4th Monitoring Survey_2026",
    "kobo_api_url": os.getenv("SC_CARBON_4TH_MONITORING_SURVEY_2026_URL"),
    "table_name": "carbon_4th_monitoring_survey_2026",
    "override_table_name": False
}


# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================
def get_clickhouse_client():
    """Create and return a ClickHouse database connection."""
    try:
        client = Client(
            host=CLICKHOUSE_CONFIG["host"],
            port=CLICKHOUSE_CONFIG["port"],
            user=CLICKHOUSE_CONFIG["username"],
            password=CLICKHOUSE_CONFIG["password"],
            database=CLICKHOUSE_CONFIG["database"],
            secure=CLICKHOUSE_CONFIG["secure"],
            verify=CLICKHOUSE_CONFIG["verify"],
            settings={'use_numpy': False}
        )
        # Test connection
        client.execute('SELECT 1')
        logger.info("ClickHouse connection established successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise


def table_exists(client, table_name):
    """Check if a table already exists in the database."""
    try:
        query = f"""
            SELECT count() 
            FROM system.tables 
            WHERE database = '{CLICKHOUSE_CONFIG["database"]}' 
            AND name = '{table_name}'
        """
        result = client.execute(query)
        exists = result[0][0] > 0
        return exists
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        return False


def create_table_if_not_exists(client, table_name, override=False):
    """Create table for a specific form if it doesn't exist, or override if specified."""
    try:
        # Create database if it doesn't exist
        client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_CONFIG['database']}")
        
        # Check if table exists
        table_already_exists = table_exists(client, table_name)
        
        if table_already_exists and not override:
            logger.info(f"Table '{table_name}' already exists and override is False - skipping table creation")
            return
        
        if table_already_exists and override:
            # Drop existing table
            logger.warning(f"Dropping existing table '{table_name}' (override enabled)")
            drop_query = f"DROP TABLE IF EXISTS {CLICKHOUSE_CONFIG['database']}.{table_name}"
            client.execute(drop_query)
            logger.info(f"Table '{table_name}' dropped successfully")
        
        # Create table with ReplacingMergeTree engine for deduplication
        # ReplacingMergeTree will keep the latest version based on updated_at
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_CONFIG['database']}.{table_name} (
                id UInt64,
                submission_id String,
                data String,
                submitted_at Nullable(DateTime),
                synced_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (submission_id)
            PRIMARY KEY (submission_id)
        """
        
        client.execute(create_table_query)
        
        if table_already_exists and override:
            logger.info(f"Table '{table_name}' recreated successfully in database '{CLICKHOUSE_CONFIG['database']}'")
        else:
            logger.info(f"Table '{table_name}' created in database '{CLICKHOUSE_CONFIG['database']}'")
        
    except Exception as e:
        logger.error(f"Error creating table '{table_name}': {e}")
        raise


def close_clickhouse_connection(client):
    """Close the ClickHouse database connection."""
    if client:
        client.disconnect()
        logger.info("ClickHouse connection closed")


# ============================================================================
# KOBO API FUNCTIONS
# ============================================================================
def test_kobo_connection(kobo_api_url):
    """Test connection to KoboToolbox API."""
    try:
        headers = {
            "Authorization": f"Token {KOBO_API_TOKEN}",
            "Accept": "application/json"
        }
        response = requests.get(kobo_api_url, headers=headers, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully connected to KoboToolbox API: {kobo_api_url}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to connect to KoboToolbox API ({kobo_api_url}): {e}")
        raise


def fetch_kobo_data(kobo_api_url, limit=None):
    """Fetch data from KoboToolbox API for a specific form."""
    try:
        headers = {
            "Authorization": f"Token {KOBO_API_TOKEN}",
            "Accept": "application/json"
        }
        
        all_results = []
        url = kobo_api_url
        params = {"limit": 1000}  # Fetch in batches
        
        while url:
            logger.info(f"Fetching data from: {url}")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)
            
            logger.info(f"Fetched {len(results)} submissions (Total: {len(all_results)})")
            
            # Check for next page
            url = data.get("next")
            params = None  # Params are included in the next URL
            
            if limit and len(all_results) >= limit:
                all_results = all_results[:limit]
                break
        
        logger.info(f"Total submissions fetched: {len(all_results)}")
        return all_results
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from KoboToolbox: {e}")
        raise


# ============================================================================
# DATA SYNC FUNCTIONS
# ============================================================================
def sync_data_to_clickhouse(client, submissions, table_name):
    """Sync KoboToolbox submissions to ClickHouse database using batch operations."""
    inserted = 0
    
    try:
        # Prepare data for batch insert
        batch_data = []
        total = len(submissions)
        
        logger.info(f"Preparing {total} submissions for batch sync...")
        
        # Get the maximum ID from the table for auto-increment
        try:
            max_id_result = client.execute(f"SELECT max(id) FROM {CLICKHOUSE_CONFIG['database']}.{table_name}")
            max_id = max_id_result[0][0] if max_id_result[0][0] is not None else 0
        except:
            max_id = 0
        
        for idx, submission in enumerate(submissions, 1):
            submission_id = submission.get("_id") or submission.get("_uuid")
            submitted_at = submission.get("_submission_time") or submission.get("end")
            
            # Convert datetime string to datetime object
            if submitted_at:
                try:
                    # Parse ISO format datetime
                    submitted_at_dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                except:
                    submitted_at_dt = None
            else:
                submitted_at_dt = None
            
            # Generate ID
            record_id = max_id + idx
            
            # Prepare row data
            batch_data.append({
                'id': record_id,
                'submission_id': str(submission_id),
                'data': json.dumps(submission),
                'submitted_at': submitted_at_dt,
                'synced_at': datetime.now(),
                'updated_at': datetime.now()
            })
            
            # Log progress every 500 submissions
            if idx % 500 == 0:
                logger.info(f"Prepared {idx}/{total} submissions...")
        
        logger.info(f"Syncing {len(batch_data)} submissions to ClickHouse...")
        
        # Insert data in batches using ClickHouse's native format
        # ReplacingMergeTree will handle deduplication based on submission_id
        insert_query = f"""
            INSERT INTO {CLICKHOUSE_CONFIG['database']}.{table_name} 
            (id, submission_id, data, submitted_at, synced_at, updated_at) 
            VALUES
        """
        
        # Batch insert
        client.execute(insert_query, batch_data)
        
        inserted = len(batch_data)
        
        logger.info(f"Sync to '{table_name}' complete: {inserted} rows inserted")
        logger.info(f"Note: Duplicates will be automatically merged by ClickHouse's ReplacingMergeTree engine")
        
        return {"inserted": inserted, "updated": 0}
        
    except Exception as e:
        logger.error(f"Error syncing data to table '{table_name}': {e}")
        raise


def sync_single_form(client, form_config):
    """Sync a single form from KoboToolbox to ClickHouse."""
    form_name = form_config["name"]
    kobo_api_url = form_config["kobo_api_url"]
    table_name = form_config["table_name"]
    override_table = form_config.get("override_table_name", False)  # Default to False
    
    logger.info(f"\n{'='*80}")
    logger.info(f"Processing form: {form_name}")
    logger.info(f"Table: {table_name} | Override: {override_table}")
    logger.info(f"{'='*80}")
    
    try:
        # Validate configuration
        if not kobo_api_url:
            logger.warning(f"Skipping '{form_name}': API URL not configured")
            return {"status": "skipped", "reason": "API URL not configured"}
        
        # Check if table exists and override is False
        if table_exists(client, table_name) and not override_table:
            logger.warning(f"Table '{table_name}' already exists and override is False")
            logger.warning(f"Skipping '{form_name}' to prevent data loss")
            logger.info(f"To sync this form, set 'override_table_name': True in configuration")
            return {"status": "skipped", "reason": "Table exists and override disabled"}
        
        # Test KoboToolbox connection
        test_kobo_connection(kobo_api_url)
        
        # Create table (will override if override_table is True)
        create_table_if_not_exists(client, table_name, override=override_table)
        
        # Fetch data from KoboToolbox
        submissions = fetch_kobo_data(kobo_api_url)
        
        if not submissions:
            logger.warning(f"No submissions found for '{form_name}'")
            return {"status": "success", "inserted": 0, "updated": 0}
        
        # Sync data to ClickHouse
        results = sync_data_to_clickhouse(client, submissions, table_name)
        
        logger.info(f"Successfully synced '{form_name}': {results}")
        return {"status": "success", **results}
        
    except Exception as e:
        logger.error(f"Failed to sync '{form_name}': {e}")
        return {"status": "failed", "error": str(e)}


# ============================================================================
# MAIN FUNCTION
# ============================================================================
def main():
    """Main function to orchestrate the sync process for a single form."""
    client = None
    
    try:
        logger.info("="*80)
        logger.info("Starting KoboToolbox to ClickHouse Sync")
        logger.info("="*80)
        
        # Validate global configuration
        if not KOBO_API_TOKEN:
            raise ValueError("SC_KOBOTOOLBOX_API_TOKEN not found in environment variables")
        
        # Validate form configuration
        if not FORM_CONFIG.get("kobo_api_url"):
            raise ValueError(f"API URL not configured for form '{FORM_CONFIG['name']}'")
        
        logger.info(f"Form to sync: {FORM_CONFIG['name']}")
        logger.info(f"Table: {FORM_CONFIG['table_name']}")
        
        # Establish database connection
        client = get_clickhouse_client()
        
        # Sync the form
        result = sync_single_form(client, FORM_CONFIG)
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("SYNC SUMMARY")
        logger.info("="*80)
        logger.info(f"Form: {FORM_CONFIG['name']}")
        logger.info(f"Table: {FORM_CONFIG['table_name']}")
        logger.info(f"Status: {result['status'].upper()}")
        
        if result["status"] == "success" and "inserted" in result:
            logger.info(f"Inserted: {result['inserted']}, Updated: {result['updated']}")
        elif result["status"] == "failed":
            logger.error(f"Error: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        elif result["status"] == "skipped":
            logger.warning(f"Reason: {result.get('reason', 'Unknown reason')}")
        
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Sync process failed: {e}")
        sys.exit(1)
    finally:
        close_clickhouse_connection(client)


if __name__ == "__main__":
    main()