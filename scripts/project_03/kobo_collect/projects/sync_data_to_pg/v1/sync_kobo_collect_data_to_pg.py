import os
import sys
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2 import sql, extras
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
# CONFIGURATION
# ============================================================================
DB_CONFIGS = {
    "host": os.getenv("SC_REPORTING_SERVICE_PG_DB_HOST"),
    "port": int(os.getenv("SC_REPORTING_SERVICE_PG_DB_PORT", "5432")),
    "username": os.getenv("SC_REPORTING_SERVICE_PG_DB_USER"),
    "password": os.getenv("SC_REPORTING_SERVICE_PG_DB_PASSWORD"),
    "database": os.getenv("SC_REPORTING_SERVICE_PG_DB_NAME"),
    "schema": 'kobo_collect',
}

KOBO_API_TOKEN = os.getenv("SC_KOBOTOOLBOX_API_TOKEN")

# ============================================================================
# MULTI-FORM CONFIGURATION
# Define all forms to sync here
# ============================================================================
FORMS_TO_SYNC = [
    {
        "name": "External Validator Auditor Form",
        "kobo_api_url": os.getenv("SC_EXTERNAL_VALIDATOR_AUDITOR_FORM_URL"),
        "table_name": "external_validator_auditor_form",
        "overrride_table_name": False
    },
    {
        "name": "Carbon Contract Signature Recollection_2025",
        "kobo_api_url": os.getenv("SC_CARBON_CONTRACT_SIGNATURE_RECOLLECTION_2025_URL"),
        "table_name": "carbon_contract_signature_recollection_2025",
        "overrride_table_name": False
    },
    {
        "name": "Reconciliation_Carbon Data Collection",
        "kobo_api_url": os.getenv("SC_RECONCILIATION_CARBON_DATA_COLLECTION_URL"),
        "table_name": "reconciliation_carbon_data_collection",
        "overrride_table_name": False
    },
    {
        "name": "Uganda Carbon Contract Client_2023",
        "kobo_api_url": os.getenv("SC_UGANDA_CARBON_CONTRACT_CLIENT_2023_URL"),
        "table_name": "uganda_carbon_contract_client_2023",
        "overrride_table_name": False
    },
    {
        "name": "Carbon 3rd Monitoring Survey_2025",
        "kobo_api_url": os.getenv("SC_CARBON_3RD_MONITORING_SURVEY_2025_URL"),
        "table_name": "carbon_3rd_monitoring_survey_2025",
        "overrride_table_name": False
    },
    {
        "name": "Carbon 2nd Monitoring Survey_2024",
        "kobo_api_url": os.getenv("SC_CARBON_2ND_MONITORING_SURVEY_2024_URL"),
        "table_name": "carbon_2nd_monitoring_survey_2024",
        "overrride_table_name": False
    },
    {
        "name": "Uganda Baseline Fossil Fuel Usage_2024",
        "kobo_api_url": os.getenv("SC_UGANDA_BASELINE_FOSSIL_FUEL_USAGE_2024_URL"),
        "table_name": "uganda_baseline_fossil_fuel_usage_2024",
        "overrride_table_name": False
    },
    {
        "name": "Pump utility field validation_July2024",
        "kobo_api_url": os.getenv("SC_PUMP_UTILITY_FIELD_VALIDATION_JULY_2024_URL"),
        "table_name": "pump_utility_field_validation_july2024",
        "overrride_table_name": False
    },
    {
        "name": 'Carbon Contract Signature Recollection_2024',
        "kobo_api_url": os.getenv("SC_CARBON_CONTRACT_SIGNATURE_RECOLLECTION_2024_URL"),
        "table_name": "carbon_contract_signature_recollection_2024",
        "overrride_table_name": False
    },
    {
        "name": "Carbon Farmer Survey_Training",
        "kobo_api_url": os.getenv("SC_CARBON_FARMER_SURVEY_TRAINING_URL"),
        "table_name": "carbon_farmer_survey_training",
        "overrride_table_name": False
    },
    {
        "name": "Uganda Carbon Contract Client_Training",
        "kobo_api_url": os.getenv("SC_UGANDA_CARBON_CONTRACT_CLIENT_TRAINING_URL"),
        "table_name": "uganda_carbon_contract_client_training",
        "overrride_table_name": False
    },
    {
        "name": "Carbon Farmer Survey_June2023",
        "kobo_api_url": os.getenv("SC_CARBON_FARMER_SURVEY_JUNE_2023_URL"),
        "table_name": "carbon_farmer_survey_june2023",
        "overrride_table_name": False
    },
    {
        "name": "Reconciliation Form Training",
        "kobo_api_url": os.getenv("SC_RECONCILIATION_FORM_TRAINING_URL"),
        "table_name": "reconciliation_form_training",
        "overrride_table_name": False
    }
    # Add more forms here as needed

]


# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================
def get_pg_db_client():
    """Create and return a PostgreSQL database connection."""
    try:
        connection = psycopg2.connect(
            host=DB_CONFIGS["host"],
            port=DB_CONFIGS["port"],
            user=DB_CONFIGS["username"],
            password=DB_CONFIGS["password"],
            database=DB_CONFIGS["database"]
        )
        logger.info("Database connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


def table_exists(connection, table_name):
    """Check if a table already exists in the schema."""
    cursor = connection.cursor()
    try:
        check_query = sql.SQL("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name = %s
            )
        """)
        cursor.execute(check_query, (DB_CONFIGS["schema"], table_name))
        exists = cursor.fetchone()[0]
        return exists
    finally:
        cursor.close()


def create_table_if_not_exists(connection, table_name, override=False):
    """Create table for a specific form if it doesn't exist, or override if specified."""
    cursor = connection.cursor()
    
    try:
        # Create schema if it doesn't exist
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(DB_CONFIGS["schema"])
            )
        )
        
        # Check if table exists
        table_already_exists = table_exists(connection, table_name)
        
        if table_already_exists and not override:
            logger.info(f"Table '{table_name}' already exists and override is False - skipping table creation")
            return
        
        if table_already_exists and override:
            # Drop existing table
            logger.warning(f"Dropping existing table '{table_name}' (override enabled)")
            drop_query = sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                sql.Identifier(DB_CONFIGS["schema"]),
                sql.Identifier(table_name)
            )
            cursor.execute(drop_query)
            connection.commit()
            logger.info(f"Table '{table_name}' dropped successfully")
        
        # Create table
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                id SERIAL PRIMARY KEY,
                submission_id VARCHAR(255) UNIQUE,
                data JSONB,
                submitted_at TIMESTAMP,
                synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(
            sql.Identifier(DB_CONFIGS["schema"]),
            sql.Identifier(table_name)
        )
        cursor.execute(create_table_query)
        connection.commit()
        
        if table_already_exists and override:
            logger.info(f"Table '{table_name}' recreated successfully in schema '{DB_CONFIGS['schema']}'")
        else:
            logger.info(f"Table '{table_name}' created in schema '{DB_CONFIGS['schema']}'")
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Error creating table '{table_name}': {e}")
        raise
    finally:
        cursor.close()


def close_pg_db_connection(connection):
    """Close the PostgreSQL database connection."""
    if connection:
        connection.close()
        logger.info("Database connection closed")


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
def sync_data_to_postgres(connection, submissions, table_name):
    """Sync KoboToolbox submissions to PostgreSQL database using batch operations."""
    cursor = connection.cursor()
    inserted = 0
    updated = 0
    
    try:
        # Prepare data for batch insert
        batch_data = []
        total = len(submissions)
        
        logger.info(f"Preparing {total} submissions for batch sync...")
        
        for idx, submission in enumerate(submissions, 1):
            submission_id = submission.get("_id") or submission.get("_uuid")
            submitted_at = submission.get("_submission_time") or submission.get("end")
            
            # Convert datetime string to timestamp
            if submitted_at:
                try:
                    submitted_at = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                except:
                    submitted_at = None
            
            batch_data.append((
                submission_id,
                json.dumps(submission),
                submitted_at
            ))
            
            # Log progress every 500 submissions
            if idx % 500 == 0:
                logger.info(f"Prepared {idx}/{total} submissions...")
        
        logger.info(f"Syncing {len(batch_data)} submissions to database...")
        
        # Use execute_values for fast batch insert with UPSERT
        upsert_query = sql.SQL("""
            INSERT INTO {}.{} (submission_id, data, submitted_at)
            VALUES %s
            ON CONFLICT (submission_id)
            DO UPDATE SET
                data = EXCLUDED.data,
                submitted_at = EXCLUDED.submitted_at,
                updated_at = CURRENT_TIMESTAMP
        """).format(
            sql.Identifier(DB_CONFIGS["schema"]),
            sql.Identifier(table_name)
        )
        
        # Execute batch insert (much faster than individual inserts)
        extras.execute_values(
            cursor,
            upsert_query.as_string(connection),
            batch_data,
            template="(%s, %s, %s)",
            page_size=500  # Process in chunks of 500
        )
        
        # Count inserts vs updates (approximate - we count total as inserted for simplicity)
        inserted = cursor.rowcount
        
        connection.commit()
        logger.info(f"Sync to '{table_name}' complete: {inserted} rows affected (inserted/updated)")
        return {"inserted": inserted, "updated": 0}  # We can't distinguish easily with batch
        
    except Exception as e:
        connection.rollback()
        logger.error(f"Error syncing data to table '{table_name}': {e}")
        raise
    finally:
        cursor.close()


def sync_single_form(connection, form_config):
    """Sync a single form from KoboToolbox to PostgreSQL."""
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
        if table_exists(connection, table_name) and not override_table:
            logger.warning(f"Table '{table_name}' already exists and override is False")
            logger.warning(f"Skipping '{form_name}' to prevent data loss")
            logger.info(f"To sync this form, set 'override_table_name': True in configuration")
            return {"status": "skipped", "reason": "Table exists and override disabled"}
        
        # Test KoboToolbox connection
        test_kobo_connection(kobo_api_url)
        
        # Create table (will override if override_table is True)
        create_table_if_not_exists(connection, table_name, override=override_table)
        
        # Fetch data from KoboToolbox
        submissions = fetch_kobo_data(kobo_api_url)
        
        if not submissions:
            logger.warning(f"No submissions found for '{form_name}'")
            return {"status": "success", "inserted": 0, "updated": 0}
        
        # Sync data to PostgreSQL
        results = sync_data_to_postgres(connection, submissions, table_name)
        
        logger.info(f"Successfully synced '{form_name}': {results}")
        return {"status": "success", **results}
        
    except Exception as e:
        logger.error(f"Failed to sync '{form_name}': {e}")
        return {"status": "failed", "error": str(e)}


# ============================================================================
# MAIN FUNCTION
# ============================================================================
def main():
    """Main function to orchestrate the sync process for all forms."""
    connection = None
    
    try:
        logger.info("="*80)
        logger.info("Starting KoboToolbox to PostgreSQL Multi-Form Sync")
        logger.info("="*80)
        
        # Validate global configuration
        if not KOBO_API_TOKEN:
            raise ValueError("SC_KOBOTOOLBOX_API_TOKEN not found in environment variables")
        
        # Filter out forms with no API URL configured
        active_forms = [f for f in FORMS_TO_SYNC if f.get("kobo_api_url")]
        
        if not active_forms:
            logger.error("No forms configured for syncing. Please check FORMS_TO_SYNC configuration.")
            sys.exit(1)
        
        logger.info(f"Found {len(active_forms)} form(s) to sync")
        
        # Establish database connection (reuse for all forms)
        connection = get_pg_db_client()
        
        # Track overall results
        overall_results = {
            "total_forms": len(active_forms),
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "details": []
        }
        
        # Sync each form
        for form_config in active_forms:
            result = sync_single_form(connection, form_config)
            overall_results["details"].append({
                "form": form_config["name"],
                "table": form_config["table_name"],
                **result
            })
            
            if result["status"] == "success":
                overall_results["successful"] += 1
            elif result["status"] == "failed":
                overall_results["failed"] += 1
            elif result["status"] == "skipped":
                overall_results["skipped"] += 1
        
        # Print summary
        logger.info("\n" + "="*80)
        logger.info("SYNC SUMMARY")
        logger.info("="*80)
        logger.info(f"Total forms processed: {overall_results['total_forms']}")
        logger.info(f"Successful: {overall_results['successful']}")
        logger.info(f"Failed: {overall_results['failed']}")
        logger.info(f"Skipped: {overall_results['skipped']}")
        logger.info("\nDetails:")
        
        for detail in overall_results["details"]:
            status_symbol = "✓" if detail["status"] == "success" else "✗" if detail["status"] == "failed" else "○"
            logger.info(f"  {status_symbol} {detail['form']} → {detail['table']}")
            if detail["status"] == "success" and "inserted" in detail:
                logger.info(f"    Inserted: {detail['inserted']}, Updated: {detail['updated']}")
            elif detail["status"] == "failed":
                logger.info(f"    Error: {detail.get('error', 'Unknown error')}")
        
        logger.info("="*80)
        
        # Exit with error code if any form failed
        if overall_results["failed"] > 0:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Sync process failed: {e}")
        sys.exit(1)
    finally:
        close_pg_db_connection(connection)


if __name__ == "__main__":
    main()