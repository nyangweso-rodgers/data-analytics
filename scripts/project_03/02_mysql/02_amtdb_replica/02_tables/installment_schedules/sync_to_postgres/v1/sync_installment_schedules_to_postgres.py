import os
import logging
from dotenv import load_dotenv
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Any, Optional
import mysql.connector
from mysql.connector import Error as MySQLError
import psycopg2
from psycopg2 import Error as PGError, sql
from psycopg2.extras import execute_batch, RealDictCursor
import argparse
import sys
import json
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
        "host": os.getenv("SC_AMT_REPLICA_MYSQL_DB_HOST"),
        "user": os.getenv("SC_AMT_REPLICA_MYSQL_DB_USER"),
        "password": os.getenv("SC_AMT_REPLICA_MYSQL_DB_PASSWORD"),
        "port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "database": os.getenv("SC_AMT_REPLICA_MYSQL_DB_NAME"),
        "table": "installment_schedules",
    },
    "TARGET_DB": {
        "host": os.getenv("SC_REPORTING_SERVICE_PG_DB_HOST"),
        "user": os.getenv("SC_REPORTING_SERVICE_PG_DB_USER"),
        "password": os.getenv("SC_REPORTING_SERVICE_PG_DB_PASSWORD"),
        "port": int(os.getenv("PG_DB_PORT", 5432)),
        "database": os.getenv("SC_REPORTING_SERVICE_PG_DB_NAME"),
        "schema": "data_science",
        "table": "amt_installment_schedules",
    }
}

BATCH_SIZE = 10000  # Number of records to process per batch
PROGRESS_FILE = "sync_progress.json"  # File to store sync progress

# Specify which fields to fetch and sync from source table
# Leave empty [] to fetch all fields
FIELD_NAMES = [
       "id",
       "accountId",
       "payPlanId",
       "customerId",
       "installmentType",
       "paymentSequence",
       "expectedAmount",
       "expectedDate",
       "status",
    "createdAt",
    "updatedAt"
]


def validate_configs():
    """Validate all required configurations."""
    errors = []
    
    required_source = ["host", "user", "password", "database", "table"]
    required_target = ["host", "user", "password", "database", "schema", "table"]
    
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


def get_mysql_connection():
    """Create MySQL database connection."""
    try:
        config = DB_CONFIGS["SOURCE_DB"]
        
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        
        logger.info("Successfully connected to MySQL SOURCE_DB")
        return conn
    
    except MySQLError as e:
        logger.error(f"Error connecting to MySQL: {e}")
        raise


def get_postgres_connection():
    """Create PostgreSQL database connection."""
    try:
        config = DB_CONFIGS["TARGET_DB"]
        
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        
        logger.info("Successfully connected to PostgreSQL TARGET_DB")
        return conn
    
    except PGError as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise


def get_mysql_schema(connection, database: str, table: str, field_names: List[str] = None) -> List[Dict[str, Any]]:
    """Retrieve column definitions from MySQL table for specified fields."""
    try:
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT 
                COLUMN_NAME as column_name,
                DATA_TYPE as data_type,
                CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
                NUMERIC_PRECISION as numeric_precision,
                NUMERIC_SCALE as numeric_scale,
                IS_NULLABLE as is_nullable,
                COLUMN_TYPE as column_type
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        
        params = [database, table]
        
        # If specific fields are requested, filter by them
        if field_names:
            placeholders = ','.join(['%s'] * len(field_names))
            query += f" AND COLUMN_NAME IN ({placeholders})"
            params.extend(field_names)
        
        query += " ORDER BY ORDINAL_POSITION"
        
        cursor.execute(query, params)
        columns = cursor.fetchall()
        cursor.close()
        
        if not columns:
            if field_names:
                raise ValueError(f"No columns found for specified fields in {database}.{table}. "
                               f"Check that field names are correct: {field_names}")
            else:
                raise ValueError(f"Table {database}.{table} not found or has no columns")
        
        # If field_names was specified, verify all requested fields were found
        if field_names:
            found_fields = [col['column_name'] for col in columns]
            missing_fields = set(field_names) - set(found_fields)
            if missing_fields:
                raise ValueError(f"Fields not found in {database}.{table}: {missing_fields}")
        
        fields_with_types = []
        for col in columns:
            field_info = {
                'name': col['column_name'],
                'type': col['data_type'],
                'column_type': col['column_type'],
                'max_length': col['character_maximum_length'],
                'precision': col['numeric_precision'],
                'scale': col['numeric_scale'],
                'nullable': col['is_nullable'] == 'YES'
            }
            fields_with_types.append(field_info)
        
        logger.info(f"Retrieved schema for {database}.{table}: {len(fields_with_types)} columns")
        return fields_with_types
    
    except MySQLError as e:
        logger.error(f"Error retrieving MySQL schema: {e}")
        raise


def map_mysql_to_postgres_type(field_info: Dict[str, Any]) -> str:
    """Map MySQL data type to PostgreSQL data type."""
    data_type = field_info['type'].lower()
    column_type = field_info.get('column_type', '').lower()
    
    # Handle character types
    if data_type in ('varchar', 'char'):
        if field_info['max_length']:
            return f"VARCHAR({field_info['max_length']})"
        return "TEXT"
    
    if data_type in ('text', 'tinytext', 'mediumtext', 'longtext'):
        return "TEXT"
    
    # Handle numeric types
    if data_type == 'decimal' or data_type == 'numeric':
        if field_info['precision'] and field_info['scale'] is not None:
            return f"NUMERIC({field_info['precision']}, {field_info['scale']})"
        return "NUMERIC"
    
    # Handle integer types
    if data_type == 'tinyint':
        # Check if it's a boolean (tinyint(1))
        if 'tinyint(1)' in column_type:
            return "BOOLEAN"
        return "SMALLINT"
    
    if data_type == 'smallint':
        return "SMALLINT"
    
    if data_type in ('mediumint', 'int', 'integer'):
        return "INTEGER"
    
    if data_type == 'bigint':
        return "BIGINT"
    
    # Handle floating point
    if data_type == 'float':
        return "REAL"
    
    if data_type == 'double':
        return "DOUBLE PRECISION"
    
    # Handle boolean
    if data_type == 'boolean' or data_type == 'bool':
        return "BOOLEAN"
    
    # Handle date/time types
    if data_type == 'datetime':
        return "TIMESTAMP"
    
    if data_type == 'timestamp':
        return "TIMESTAMPTZ"
    
    if data_type == 'date':
        return "DATE"
    
    if data_type == 'time':
        return "TIME"
    
    if data_type == 'year':
        return "SMALLINT"
    
    # Handle JSON types
    if data_type == 'json':
        return "JSONB"
    
    # Handle binary types
    if data_type in ('binary', 'varbinary', 'blob', 'tinyblob', 'mediumblob', 'longblob'):
        return "BYTEA"
    
    # Handle enum and set as text
    if data_type in ('enum', 'set'):
        return "TEXT"
    
    # Default fallback
    logger.warning(f"Unknown MySQL type '{data_type}' (column_type: {column_type}), defaulting to TEXT")
    return "TEXT"


def create_postgres_table_if_not_exists(connection, schema: str, table: str, 
                                       fields_with_types: List[Dict[str, str]]):
    """Create PostgreSQL table with proper data types based on MySQL source schema."""
    try:
        cursor = connection.cursor()
        
        # Create schema if it doesn't exist
        cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(schema)
        ))
        
        # Build column definitions with proper quoting to preserve case
        column_defs = []
        for field in fields_with_types:
            pg_type = map_mysql_to_postgres_type(field)
            nullable = "NULL" if field['nullable'] else "NOT NULL"
            # Use sql.Identifier to properly quote column names
            col_def = sql.SQL("{} {} {}").format(
                sql.Identifier(field['name']),
                sql.SQL(pg_type),
                sql.SQL(nullable)
            )
            column_defs.append(col_def)
        
        # Add primary key constraint on 'id' column if it exists
        has_id_column = any(field['name'] == 'id' for field in fields_with_types)
        if has_id_column:
            column_defs.append(sql.SQL("PRIMARY KEY (id)"))
        
        # Create table
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            )
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(column_defs)
        )
        
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        
        logger.info(f"Table {schema}.{table} created or already exists with {len(fields_with_types)} columns")
    
    except PGError as e:
        logger.error(f"Error creating PostgreSQL table: {e}")
        connection.rollback()
        raise


def fetch_data_from_mysql(connection, database: str, table: str, 
                          field_names: List[str], offset: int, limit: int) -> List[Dict[str, Any]]:
    """Fetch data from MySQL database table in batches for specified fields only."""
    try:
        cursor = connection.cursor(dictionary=True)
        
        # Build SELECT query with specific fields or all fields
        if field_names:
            columns = ", ".join([f"`{field}`" for field in field_names])
        else:
            columns = "*"
        
        query = f"SELECT {columns} FROM `{database}`.`{table}` LIMIT %s OFFSET %s"
        
        cursor.execute(query, (limit, offset))
        rows = cursor.fetchall()
        cursor.close()
        
        logger.info(f"Fetched {len(rows)} records from {database}.{table} (offset: {offset})")
        return rows
    
    except MySQLError as e:
        logger.error(f"Error fetching data from MySQL: {e}")
        raise


def sync_data_to_postgres(connection, schema: str, table: str, 
                          data: List[Dict[str, Any]], field_names: List[str]):
    """Sync data to PostgreSQL database table."""
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
    
    except PGError as e:
        logger.error(f"Error syncing data to PostgreSQL: {e}")
        connection.rollback()
        raise


def get_mysql_row_count(connection, database: str, table: str) -> int:
    """Get total row count from MySQL table."""
    try:
        cursor = connection.cursor()
        query = f"SELECT COUNT(*) FROM `{database}`.`{table}`"
        cursor.execute(query)
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except MySQLError as e:
        logger.error(f"Error getting MySQL row count: {e}")
        raise


def get_postgres_row_count(connection, schema: str, table: str) -> int:
    """Get total row count from PostgreSQL table."""
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
    except PGError as e:
        logger.error(f"Error getting PostgreSQL row count: {e}")
        raise


def postgres_table_exists(connection, schema: str, table: str) -> bool:
    """Check if a table exists in PostgreSQL database."""
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
    except PGError as e:
        logger.error(f"Error checking PostgreSQL table existence: {e}")
        raise


def verify_sync(source_conn, target_conn):
    """Verify synced data between MySQL and PostgreSQL."""
    try:
        source_config = DB_CONFIGS["SOURCE_DB"]
        target_config = DB_CONFIGS["TARGET_DB"]
        
        # Check if target table exists
        if not postgres_table_exists(
            target_conn,
            target_config["schema"],
            target_config["table"]
        ):
            logger.error(f"Target table {target_config['schema']}.{target_config['table']} does not exist")
            logger.info("Run the script without --verify-only to create the table and sync data")
            return
        
        source_count = get_mysql_row_count(
            source_conn, 
            source_config["database"],
            source_config["table"]
        )
        
        target_count = get_postgres_row_count(
            target_conn,
            target_config["schema"],
            target_config["table"]
        )
        
        logger.info(f"Source (MySQL) table row count: {source_count}")
        logger.info(f"Target (PostgreSQL) table row count: {target_count}")
        
        if source_count == target_count:
            logger.info("✓ Sync verification successful: Row counts match")
        else:
            logger.warning(f"⚠ Row count mismatch: Source={source_count}, Target={target_count}")
    
    except Exception as e:
        logger.error(f"Error verifying sync: {e}")
        raise


def save_progress(offset: int, total_synced: int, total_rows: int):
    """Save sync progress to a JSON file."""
    progress_data = {
        "offset": offset,
        "total_synced": total_synced,
        "total_rows": total_rows,
        "last_updated": datetime.now().isoformat(),
        "source_table": f"{DB_CONFIGS['SOURCE_DB']['database']}.{DB_CONFIGS['SOURCE_DB']['table']}",
        "target_table": f"{DB_CONFIGS['TARGET_DB']['schema']}.{DB_CONFIGS['TARGET_DB']['table']}"
    }
    
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress_data, f, indent=2)
        logger.debug(f"Progress saved: offset={offset}, synced={total_synced}/{total_rows}")
    except Exception as e:
        logger.warning(f"Failed to save progress: {e}")


def load_progress() -> Optional[Dict[str, Any]]:
    """Load sync progress from JSON file."""
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                progress_data = json.load(f)
            
            # Verify it's for the same tables
            current_source = f"{DB_CONFIGS['SOURCE_DB']['database']}.{DB_CONFIGS['SOURCE_DB']['table']}"
            current_target = f"{DB_CONFIGS['TARGET_DB']['schema']}.{DB_CONFIGS['TARGET_DB']['table']}"
            
            if (progress_data.get('source_table') == current_source and 
                progress_data.get('target_table') == current_target):
                logger.info(f"Found existing progress: {progress_data['total_synced']}/{progress_data['total_rows']} "
                           f"records synced ({progress_data['total_synced']/progress_data['total_rows']*100:.1f}%)")
                logger.info(f"Last updated: {progress_data['last_updated']}")
                return progress_data
            else:
                logger.warning("Progress file is for different tables, ignoring")
                return None
        return None
    except Exception as e:
        logger.warning(f"Failed to load progress: {e}")
        return None


def clear_progress():
    """Clear the progress file."""
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            logger.info("Progress file cleared")
    except Exception as e:
        logger.warning(f"Failed to clear progress file: {e}")


def main():
    """Main function with CLI argument support."""
    parser = argparse.ArgumentParser(description='Sync MySQL table to PostgreSQL in batches')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                       help=f'Number of records per batch (default: {BATCH_SIZE})')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify sync without syncing data')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from last saved progress')
    parser.add_argument('--clear-progress', action='store_true',
                       help='Clear saved progress and start fresh')
    
    args = parser.parse_args()
    
    # Handle clear progress flag
    if args.clear_progress:
        clear_progress()
        logger.info("Use --resume to start syncing from the beginning")
        return
    
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
        source_conn = get_mysql_connection()
        target_conn = get_postgres_connection()
        
        if args.verify_only:
            verify_sync(source_conn, target_conn)
            return
        
        # Get source table schema
        logger.info("Retrieving MySQL source table schema...")
        fields_with_types = get_mysql_schema(
            source_conn,
            source_config["database"],
            source_config["table"],
            FIELD_NAMES if FIELD_NAMES else None
        )
        
        # Create target table with mapped schema
        logger.info("Creating PostgreSQL target table if not exists...")
        create_postgres_table_if_not_exists(
            target_conn,
            target_config["schema"],
            target_config["table"],
            fields_with_types
        )
        
        # Get field names for insertion
        field_names = [field['name'] for field in fields_with_types]
        
        if FIELD_NAMES:
            logger.info(f"Syncing specified fields only: {field_names}")
        else:
            logger.info(f"Syncing all fields: {len(field_names)} columns")
        
        # Get total row count for progress tracking
        total_rows = get_mysql_row_count(
            source_conn,
            source_config["database"],
            source_config["table"]
        )
        
        logger.info(f"Total rows to sync: {total_rows}")
        
        # Load progress if resuming
        offset = 0
        total_synced = 0
        
        if args.resume:
            progress = load_progress()
            if progress:
                offset = progress['offset']
                total_synced = progress['total_synced']
                logger.info(f"Resuming from offset {offset} ({total_synced} records already synced)")
            else:
                logger.info("No valid progress found, starting from beginning")
        else:
            # Clear any existing progress if not resuming
            clear_progress()
        
        # Sync data in batches
        while True:
            logger.info(f"Processing batch at offset {offset}...")
            
            # Fetch batch from MySQL source
            batch_data = fetch_data_from_mysql(
                source_conn,
                source_config["database"],
                source_config["table"],
                field_names,
                offset,
                args.batch_size
            )
            
            if not batch_data:
                logger.info("No more data to sync")
                break
            
            # Sync batch to PostgreSQL target
            sync_data_to_postgres(
                target_conn,
                target_config["schema"],
                target_config["table"],
                batch_data,
                field_names
            )
            
            total_synced += len(batch_data)
            offset += args.batch_size
            
            # Save progress after each successful batch
            save_progress(offset, total_synced, total_rows)
            
            logger.info(f"Progress: {total_synced}/{total_rows} records synced "
                       f"({total_synced/total_rows*100:.1f}%)")
            
            # Break if we got fewer records than batch size
            if len(batch_data) < args.batch_size:
                break
        
        # Clear progress file on successful completion
        clear_progress()
        
        # Verify sync
        logger.info("Verifying sync...")
        verify_sync(source_conn, target_conn)
        
        logger.info("✓ Sync completed successfully")
    
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        logger.info("Progress has been saved. Use --resume to continue from where it left off")
        sys.exit(1)
    
    finally:
        # Close connections
        if source_conn:
            source_conn.close()
            logger.info("MySQL source connection closed")
        if target_conn:
            target_conn.close()
            logger.info("PostgreSQL target connection closed")


if __name__ == "__main__":
    main()