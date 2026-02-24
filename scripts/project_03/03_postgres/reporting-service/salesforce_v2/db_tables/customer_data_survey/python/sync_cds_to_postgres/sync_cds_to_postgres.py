import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2 import Error, sql
from psycopg2.extras import execute_batch, RealDictCursor
import argparse
import sys
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ========================
# CONFIGURATION SECTION
# ========================
DB_CONFIGS = {
    "SOURCE_DB": {
        "source_db_host": os.getenv("SC_REPORTING_SERVICE_PG_DB_HOST"),
        "source_db_user": os.getenv("SC_REPORTING_SERVICE_PG_DB_USER"),
        "source_db_password": os.getenv("SC_REPORTING_SERVICE_PG_DB_PASSWORD"),
        "port": int(os.getenv("PG_DB_PORT", 5432)),
        "source_db_name": "reporting-service",
        "source_db_schema": "salesforce_v2",
        "source_table": "customer_data_survey",
    },
    "TARGET_DB": {
        "target_db_host": os.getenv("SC_REPORTING_SERVICE_PG_DB_HOST"),
        "target_db_user": os.getenv("SC_REPORTING_SERVICE_PG_DB_USER"),
        "target_db_password": os.getenv("SC_REPORTING_SERVICE_PG_DB_PASSWORD"),
        "port": int(os.getenv("PG_DB_PORT", 5432)),
        "target_db_name": "reporting-service",
        "target_db_schema": "data_science",
        "target_table": "customer_data_survey",
    }
}

BATCH_SIZE = 1000  # Number of records to process per batch

# Specify which fields to fetch and sync from source table
# Leave empty [] to fetch all fields
FIELD_NAMES = [
    "id",
    "mobile_number",
    "number_of_cows",
    "number_of_goats",
    "been_living_in_the_same_location_for",
    "number_of_pigs",
    "number_of_sheep",
    "how_long_have_you_been_a_farmer",
    "do_you_have_animals",
    "type_of_fruits_and_vegetables_grown",
    "pest_disease_control_pest_type_usage",
    "other_machinery_and_equipment_ownership",
    "salary_amount",
    "main_purpose_of_acquiring_the_product",
    "primary_decision_maker_to_buy_product",
    "main_source_of_income",
    "periodicity_of_payment",
    "total_amount_from_pension",
    "total_amount_from_salary_government",
    "how_long_have_you_had_the_same_phone_no",
    "living_in_the_same_location_for",
    "amount_left_after_monthly_expenses",
    "harvest_cycle_per_year",
    "farm_acreage",
    "main_purpose_of_acquiring_the_pump",
    "other_sources_of_water",
    "hours_spent_fetching_water_every_week",
    "amount_paid_for_getting_water_each_week",
    "amount_paid_for_water_other_pump_usage",
    "quantity_of_water_usage_per_week",
    "water_tank_capacity",
    "electricity_connectivity",
    "number_of_financial_dependants",
    "number_of_working_age_adults_in_the_hh",
    "average_monthly_income",
    "amount_spent_on_school_fees",
    "amount_spent_on_food",
    "amount_spent_on_farm_inputs",
    "amount_spent_on_rent",
    "amount_spent_on_loans",
    "amount_spent_on_other",
    "currently_have_any_outstanding_loans",
    "total_amount_of_outstanding_loan_s",
    "no_of_months_to_finish_paying_loan_s",
    "preferred_banking_method",
    "number_of_loans_taken_in_the_last_2yrs",
    "periodicity_of_the_income",
]


def validate_configs():
    """Validate all required configurations."""
    errors = []
    
    required_source = ["source_db_host", "source_db_user", "source_db_password", 
                       "source_db_name", "source_db_schema", "source_table"]
    required_target = ["target_db_host", "target_db_user", "target_db_password", 
                       "target_db_name", "target_db_schema", "target_table"]
    
    for field in required_source:
        if not DB_CONFIGS["SOURCE_DB"].get(field):
            errors.append(f"Missing SOURCE_DB configuration: {field}")
    
    for field in required_target:
        if not DB_CONFIGS["TARGET_DB"].get(field):
            errors.append(f"Missing TARGET_DB configuration: {field}")
    
    if errors:
        for error in errors:
            logger.error(error)
        raise ValueError("Configuration validation failed. Check logs for details.")
    
    logger.info("Configuration validated successfully")


def get_postgres_connection(db_type: str):
    """Create PostgreSQL database connection."""
    try:
        config = DB_CONFIGS[db_type]
        
        if db_type == "SOURCE_DB":
            conn = psycopg2.connect(
                host=config["source_db_host"],
                port=config["port"],
                database=config["source_db_name"],
                user=config["source_db_user"],
                password=config["source_db_password"]
            )
        else:  # TARGET_DB
            conn = psycopg2.connect(
                host=config["target_db_host"],
                port=config["port"],
                database=config["target_db_name"],
                user=config["target_db_user"],
                password=config["target_db_password"]
            )
        
        logger.info(f"Successfully connected to {db_type}")
        return conn
    
    except Error as e:
        logger.error(f"Error connecting to {db_type}: {e}")
        raise


def get_source_schema(connection, schema: str, table: str, field_names: List[str] = None) -> List[Dict[str, str]]:
    """Retrieve column definitions from source table for specified fields."""
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        
        params = [schema, table]
        
        # If specific fields are requested, filter by them
        if field_names:
            placeholders = ','.join(['%s'] * len(field_names))
            query += f" AND column_name IN ({placeholders})"
            params.extend(field_names)
        
        query += " ORDER BY ordinal_position"
        
        cursor.execute(query, params)
        columns = cursor.fetchall()
        cursor.close()
        
        if not columns:
            if field_names:
                raise ValueError(f"No columns found for specified fields in {schema}.{table}. "
                               f"Check that field names are correct: {field_names}")
            else:
                raise ValueError(f"Table {schema}.{table} not found or has no columns")
        
        # If field_names was specified, verify all requested fields were found
        if field_names:
            found_fields = [col['column_name'] for col in columns]
            missing_fields = set(field_names) - set(found_fields)
            if missing_fields:
                raise ValueError(f"Fields not found in {schema}.{table}: {missing_fields}")
        
        fields_with_types = []
        for col in columns:
            field_info = {
                'name': col['column_name'],
                'type': col['data_type'],
                'max_length': col['character_maximum_length'],
                'precision': col['numeric_precision'],
                'scale': col['numeric_scale'],
                'nullable': col['is_nullable'] == 'YES'
            }
            fields_with_types.append(field_info)
        
        logger.info(f"Retrieved schema for {schema}.{table}: {len(fields_with_types)} columns")
        return fields_with_types
    
    except Error as e:
        logger.error(f"Error retrieving schema: {e}")
        raise


def map_postgres_type(field_info: Dict[str, Any]) -> str:
    """Map PostgreSQL data type with proper constraints."""
    data_type = field_info['type'].lower()
    
    # Handle character types
    if data_type in ('character varying', 'varchar'):
        if field_info['max_length']:
            return f"VARCHAR({field_info['max_length']})"
        return "TEXT"
    
    if data_type in ('character', 'char'):
        if field_info['max_length']:
            return f"CHAR({field_info['max_length']})"
        return "CHAR(1)"
    
    if data_type == 'text':
        return "TEXT"
    
    # Handle numeric types
    if data_type == 'numeric' or data_type == 'decimal':
        if field_info['precision'] and field_info['scale']:
            return f"NUMERIC({field_info['precision']}, {field_info['scale']})"
        return "NUMERIC"
    
    # Handle integer types
    if data_type in ('integer', 'int', 'int4'):
        return "INTEGER"
    
    if data_type in ('bigint', 'int8'):
        return "BIGINT"
    
    if data_type in ('smallint', 'int2'):
        return "SMALLINT"
    
    # Handle floating point
    if data_type in ('double precision', 'float8'):
        return "DOUBLE PRECISION"
    
    if data_type in ('real', 'float4'):
        return "REAL"
    
    # Handle boolean
    if data_type in ('boolean', 'bool'):
        return "BOOLEAN"
    
    # Handle date/time types
    if data_type == 'timestamp without time zone':
        return "TIMESTAMP"
    
    if data_type == 'timestamp with time zone':
        return "TIMESTAMPTZ"
    
    if data_type == 'date':
        return "DATE"
    
    if data_type == 'time without time zone':
        return "TIME"
    
    if data_type == 'time with time zone':
        return "TIMETZ"
    
    # Handle JSON types
    if data_type == 'json':
        return "JSON"
    
    if data_type == 'jsonb':
        return "JSONB"
    
    # Handle UUID
    if data_type == 'uuid':
        return "UUID"
    
    # Handle arrays
    if data_type == 'ARRAY':
        return "TEXT[]"
    
    # Default fallback
    logger.warning(f"Unknown type '{data_type}', defaulting to TEXT")
    return "TEXT"


def create_postgres_table_if_not_exists(connection, schema: str, table: str, 
                                       fields_with_types: List[Dict[str, str]]):
    """Create PostgreSQL table with proper data types based on source schema."""
    try:
        cursor = connection.cursor()
        
        # Create schema if it doesn't exist
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema)
        ))
        
        # Build column definitions
        column_defs = []
        for field in fields_with_types:
            pg_type = map_postgres_type(field)
            nullable = "NULL" if field['nullable'] else "NOT NULL"
            column_defs.append(f"{field['name']} {pg_type} {nullable}")
        
        # Create table
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            )
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ".join(column_defs))
        )
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        
        logger.info(f"Table {schema}.{table} created or already exists with {len(fields_with_types)} columns")
    
    except Error as e:
        logger.error(f"Error creating table: {e}")
        connection.rollback()
        raise


def fetch_data_from_source_db(connection, schema: str, table: str, 
                              field_names: List[str], offset: int, limit: int) -> List[Dict[str, Any]]:
    """Fetch data from source PostgreSQL database table in batches for specified fields only."""
    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Build SELECT query with specific fields or all fields
        if field_names:
            columns = sql.SQL(", ").join([sql.Identifier(field) for field in field_names])
        else:
            columns = sql.SQL("*")
        
        query = sql.SQL("SELECT {} FROM {}.{} ORDER BY 1 OFFSET %s LIMIT %s").format(
            columns,
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        
        cursor.execute(query, (offset, limit))
        rows = cursor.fetchall()
        cursor.close()
        
        # Convert RealDictRow to regular dict
        data = [dict(row) for row in rows]
        
        logger.info(f"Fetched {len(data)} records from {schema}.{table} (offset: {offset})")
        return data
    
    except Error as e:
        logger.error(f"Error fetching data: {e}")
        raise


def sync_data_to_target_db(connection, schema: str, table: str, 
                           data: List[Dict[str, Any]], field_names: List[str]):
    """Sync data to target PostgreSQL database table."""
    if not data:
        logger.info("No data to sync")
        return
    
    try:
        cursor = connection.cursor()
        
        # Build INSERT query with ON CONFLICT handling
        columns = sql.SQL(", ").join([sql.Identifier(field) for field in field_names])
        placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(field_names))
        
        insert_query = sql.SQL("""
            INSERT INTO {}.{} ({})
            VALUES ({})
            ON CONFLICT DO NOTHING
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            columns,
            placeholders
        )
        
        # Prepare data tuples
        data_tuples = []
        for record in data:
            values = tuple(record.get(field) for field in field_names)
            data_tuples.append(values)
        
        # Execute batch insert
        execute_batch(cursor, insert_query, data_tuples, page_size=500)
        connection.commit()
        cursor.close()
        
        logger.info(f"Successfully synced {len(data)} records to {schema}.{table}")
    
    except Error as e:
        logger.error(f"Error syncing data: {e}")
        connection.rollback()
        raise


def get_row_count(connection, schema: str, table: str) -> int:
    """Get total row count from a table."""
    try:
        cursor = connection.cursor()
        query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table)
        )
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Error as e:
        logger.error(f"Error getting row count: {e}")
        raise


def table_exists(connection, schema: str, table: str) -> bool:
    """Check if a table exists in the database."""
    try:
        cursor = connection.cursor()
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """
        cursor.execute(query, (schema, table))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
    except Error as e:
        logger.error(f"Error checking table existence: {e}")
        raise


def verify_sync(source_conn, target_conn):
    """Verify synced data in PostgreSQL."""
    try:
        source_config = DB_CONFIGS["SOURCE_DB"]
        target_config = DB_CONFIGS["TARGET_DB"]
        
        # Check if target table exists
        if not table_exists(
            target_conn,
            target_config["target_db_schema"],
            target_config["target_table"]
        ):
            logger.error(f"Target table {target_config['target_db_schema']}.{target_config['target_table']} does not exist")
            logger.info("Run the script without --verify-only to create the table and sync data")
            return
        
        source_count = get_row_count(
            source_conn, 
            source_config["source_db_schema"],
            source_config["source_table"]
        )
        
        target_count = get_row_count(
            target_conn,
            target_config["target_db_schema"],
            target_config["target_table"]
        )
        
        logger.info(f"Source table row count: {source_count}")
        logger.info(f"Target table row count: {target_count}")
        
        if source_count == target_count:
            logger.info("✓ Sync verification successful: Row counts match")
        else:
            logger.warning(f"⚠ Row count mismatch: Source={source_count}, Target={target_count}")
    
    except Error as e:
        logger.error(f"Error verifying sync: {e}")
        raise


def main():
    """Main function with CLI argument support."""
    parser = argparse.ArgumentParser(description='Sync PostgreSQL tables in batches')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                       help=f'Number of records per batch (default: {BATCH_SIZE})')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify sync without syncing data')
    
    args = parser.parse_args()
    
    source_conn = None
    target_conn = None
    
    try:
        # Validate configurations
        validate_configs()
        
        # Get configuration
        source_config = DB_CONFIGS["SOURCE_DB"]
        target_config = DB_CONFIGS["TARGET_DB"]
        
        # Connect to databases
        logger.info("Connecting to databases...")
        source_conn = get_postgres_connection("SOURCE_DB")
        target_conn = get_postgres_connection("TARGET_DB")
        
        if args.verify_only:
            verify_sync(source_conn, target_conn)
            return
        
        # Get source table schema
        logger.info("Retrieving source table schema...")
        fields_with_types = get_source_schema(
            source_conn,
            source_config["source_db_schema"],
            source_config["source_table"],
            FIELD_NAMES if FIELD_NAMES else None
        )
        
        # Create target table with same schema
        logger.info("Creating target table if not exists...")
        create_postgres_table_if_not_exists(
            target_conn,
            target_config["target_db_schema"],
            target_config["target_table"],
            fields_with_types
        )
        
        # Get field names for insertion
        field_names = [field['name'] for field in fields_with_types]
        
        if FIELD_NAMES:
            logger.info(f"Syncing specified fields only: {field_names}")
        else:
            logger.info(f"Syncing all fields: {len(field_names)} columns")
        
        # Get total row count for progress tracking
        total_rows = get_row_count(
            source_conn,
            source_config["source_db_schema"],
            source_config["source_table"]
        )
        
        logger.info(f"Total rows to sync: {total_rows}")
        
        # Sync data in batches
        offset = 0
        total_synced = 0
        
        while True:
            logger.info(f"Processing batch at offset {offset}...")
            
            # Fetch batch from source
            batch_data = fetch_data_from_source_db(
                source_conn,
                source_config["source_db_schema"],
                source_config["source_table"],
                field_names,
                offset,
                args.batch_size
            )
            
            if not batch_data:
                logger.info("No more data to sync")
                break
            
            # Sync batch to target
            sync_data_to_target_db(
                target_conn,
                target_config["target_db_schema"],
                target_config["target_table"],
                batch_data,
                field_names
            )
            
            total_synced += len(batch_data)
            logger.info(f"Progress: {total_synced}/{total_rows} records synced "
                       f"({total_synced/total_rows*100:.1f}%)")
            
            # Move to next batch
            offset += args.batch_size
            
            # Break if we got fewer records than batch size
            if len(batch_data) < args.batch_size:
                break
        
        # Verify sync
        logger.info("Verifying sync...")
        verify_sync(source_conn, target_conn)
        
        logger.info("✓ Sync completed successfully")
    
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        sys.exit(1)
    
    finally:
        # Close connections
        if source_conn:
            source_conn.close()
            logger.info("Source connection closed")
        if target_conn:
            target_conn.close()
            logger.info("Target connection closed")


if __name__ == "__main__":
    main()