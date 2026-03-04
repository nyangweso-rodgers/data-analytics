import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time
import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "host": os.getenv("SC_REPORTING_SERVICE_PG_DB_HOST"),
    "port": int(os.getenv("POSTGRES_DB_PORT", "5432")),
    "user": os.getenv("SC_REPORTING_SERVICE_PG_DB_USER"),
    "password": os.getenv("SC_REPORTING_SERVICE_PG_DB_PASSWORD"),
    "database": "reporting-service"
}

# Schema to use for tables
POSTGRES_SCHEMA = "data_science"

# PostgreSQL Table Configurations
POSTGRES_TABLE_CONFIGS = {
    "target_table_name": "raw_location",
    "primary_key": [],  # TODO: Add primary key if needed
    "indexes": [
        []  # TODO: Add indexes if needed
    ],
    "batch_size": 50000
}

FILE_PATH = "../../../../../../../../location.csv"  #TODO: Update with actual file path or pass as argument

# Field mapping: column_name -> postgres_data_type
FIELD_MAPPING = {
    'customerid': 'INTEGER',  # Original: customerId
    'longitude': 'DOUBLE PRECISION',
    'latitude': 'DOUBLE PRECISION',
}


# Column rename mapping (if needed for reading CSV)
COLUMN_RENAME_MAPPING = {
    'customerId': 'customerid',
}


def get_postgres_client():
    """Establish connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def check_postgres_table_exists(conn, schema: str, table_name: str) -> bool:
    """Check if table exists in PostgreSQL"""
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """)
            cursor.execute(query, (schema, table_name))
            exists = cursor.fetchone()[0]
            logger.info(f"Table {schema}.{table_name} exists: {exists}")
            return exists
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        raise


def create_postgres_table(conn, schema: str, table_name: str):
    """Create table in PostgreSQL if it does not exist"""
    try:
        with conn.cursor() as cursor:
            # Create schema if not exists
            cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                sql.Identifier(schema)
            ))
            
            # Build column definitions
            column_defs = []
            for field_name, data_type in FIELD_MAPPING.items():
                column_defs.append(f"{field_name} {data_type}")
            
            # Add metadata columns
            column_defs.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            column_defs.append("updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            
            columns_sql = ", ".join(column_defs)
            
            # Create table
            create_table_query = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    {}
                )
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.SQL(columns_sql)
            )
            
            cursor.execute(create_table_query)
            
            # Create indexes if specified
            if POSTGRES_TABLE_CONFIGS.get("indexes"):
                for index_cols in POSTGRES_TABLE_CONFIGS["indexes"]:
                    if index_cols:  # Skip empty lists
                        index_name = f"idx_{table_name}_{'_'.join(index_cols)}"
                        index_query = sql.SQL("""
                            CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})
                        """).format(
                            sql.Identifier(index_name),
                            sql.Identifier(schema),
                            sql.Identifier(table_name),
                            sql.SQL(', ').join(map(sql.Identifier, index_cols))
                        )
                        cursor.execute(index_query)
                        logger.info(f"Created index: {index_name}")
            
            conn.commit()
            logger.info(f"Table {schema}.{table_name} created successfully")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating table: {e}")
        raise


def read_data_from_file(file_path: str) -> pd.DataFrame:
    """Read data from CSV or Excel file"""
    try:
        logger.info(f"Reading data from {file_path}")
        
        # Determine file type and read accordingly
        if file_path.endswith('.csv'):
            # Use low_memory=False to avoid mixed type warnings
            df = pd.read_csv(file_path, low_memory=False)
        elif file_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        logger.info(f"Successfully read {len(df)} rows from file")
        logger.info(f"Columns in file: {df.columns.tolist()}")
        
        # Rename columns to PostgreSQL-compatible names
        if 'COLUMN_RENAME_MAPPING' in globals() and COLUMN_RENAME_MAPPING:
            logger.info("Renaming columns to PostgreSQL-compatible names")
            df = df.rename(columns=COLUMN_RENAME_MAPPING)
            logger.info(f"Renamed columns: {df.columns.tolist()}")
        
        return df
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise


def clean_and_transform_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Clean and transform data to match PostgreSQL schema"""
    try:
        logger.info("Cleaning and transforming data")
        
        # Helper function to parse dates with multiple format attempts
        def parse_date_column(df, col):
            if col not in df.columns:
                return df
            
            logger.info(f"Parsing date column: {col}")
            sample_vals = df[col].head(5).tolist()
            logger.info(f"Sample values before parsing: {sample_vals}")
            logger.info(f"Data type: {df[col].dtype}")
            
            # Special handling for '17-May' format (year-month)
            if col in ['schedule_month_year', 'schedule_month_started']:
                # First try standard pandas parsing with %y-%b format
                try:
                    parsed = pd.to_datetime(df[col], format='%y-%b', errors='coerce')
                    valid_count = parsed.notna().sum()
                    logger.info(f"Attempted %y-%b parsing: {valid_count}/{len(df)} valid ({valid_count/len(df)*100:.1f}%)")
                    
                    if valid_count > len(df) * 0.5:
                        df[col] = parsed
                        logger.info(f"Successfully parsed {col} with format: %y-%b")
                        logger.info(f"Sample parsed dates: {df[col].head(3).tolist()}")
                        return df
                except Exception as e:
                    logger.warning(f"Standard %y-%b parsing failed: {e}")
                
                # If standard parsing didn't work, try manual parsing
                logger.info(f"Attempting manual parsing for {col}")
                try:
                    # Convert '17-May' to '01-May-2017' for better parsing
                    def manual_parse_year_month(val):
                        if pd.isna(val) or val == '' or str(val).lower() == 'nan':
                            return None
                        try:
                            # Handle '17-May' format (year-month)
                            val_str = str(val).strip()
                            parts = val_str.split('-')
                            if len(parts) == 2:
                                year_str, month_str = parts
                                # Convert 2-digit year to 4-digit
                                year = int(year_str)
                                if year < 100:
                                    year = 2000 + year if year < 50 else 1900 + year
                                # Parse month - set to first day of month
                                date_str = f"01-{month_str}-{year}"
                                return pd.to_datetime(date_str, format='%d-%b-%Y', errors='coerce')
                        except:
                            return None
                        return None
                    
                    df[col] = df[col].apply(manual_parse_year_month)
                    valid_count = df[col].notna().sum()
                    logger.info(f"Manual parsing results: {valid_count}/{len(df)} valid ({valid_count/len(df)*100:.1f}%)")
                    logger.info(f"Sample parsed dates: {df[col].head(5).tolist()}")
                    return df
                    
                except Exception as e:
                    logger.error(f"Manual parsing failed: {e}")
            
            # Try multiple date formats for other columns
            formats_to_try = [
                '%d/%m/%Y',      # 06/05/2017
                '%Y-%m-%d',      # 2026-05-17
                '%m/%d/%Y',      # 05/06/2017
                '%Y/%m/%d',      # 2017/05/06
                '%d-%m-%Y',      # 06-05-2017
                '%m-%d-%Y',      # 05-06-2017
            ]
            
            for fmt in formats_to_try:
                try:
                    parsed = pd.to_datetime(df[col], format=fmt, errors='coerce')
                    # Check if we got valid results
                    if parsed.notna().sum() > len(df) * 0.5:  # More than 50% valid
                        logger.info(f"Successfully parsed {col} with format: {fmt}")
                        logger.info(f"Valid dates: {parsed.notna().sum()}/{len(df)} ({parsed.notna().sum()/len(df)*100:.1f}%)")
                        df[col] = parsed
                        return df
                except Exception as e:
                    continue
            
            # If no format worked well, try pandas auto-detection
            logger.warning(f"Standard formats failed for {col}, trying auto-detection")
            df[col] = pd.to_datetime(df[col], errors='coerce')
            logger.info(f"Auto-detection results - Valid dates: {df[col].notna().sum()}/{len(df)} ({df[col].notna().sum()/len(df)*100:.1f}%)")
            
            return df
        
        # Parse all date columns
        date_columns = ['schedule_month_year', 'schedule_installment_date', 'schedule_month_started']
        for col in date_columns:
            df = parse_date_column(df, col)
        
        # Clean and convert numeric columns
        numeric_columns = [col for col, dtype in FIELD_MAPPING.items() 
                          if 'NUMERIC' in dtype or 'INTEGER' in dtype or 'BIGINT' in dtype or 'DOUBLE PRECISION' in dtype]
        
        logger.info(f"Converting {len(numeric_columns)} numeric columns")
        for col in numeric_columns:
            if col in df.columns:
                # Sample before conversion
                sample_before = df[col].head(3).tolist()
                
                # Clean the column if it's object type (string)
                if df[col].dtype == 'object':
                    # Remove commas from numbers (e.g., '37,500' -> '37500')
                    df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                    # Replace dashes with NaN (often used as null indicator)
                    df[col] = df[col].str.replace('-', '', regex=False)
                    # Replace empty strings with NaN
                    df[col] = df[col].replace('', None)
                    df[col] = df[col].replace('nan', None)
                    df[col] = df[col].replace('NaN', None)
                    df[col] = df[col].replace('NULL', None)
                    df[col] = df[col].replace('null', None)
                
                # Convert to numeric
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Check for BIGINT overflow (PostgreSQL BIGINT max: 9223372036854775807)
                if 'BIGINT' in FIELD_MAPPING[col]:
                    max_val = df[col].max()
                    min_val = df[col].min()
                    if pd.notna(max_val) and max_val > 9223372036854775807:
                        logger.error(f"BIGINT OVERFLOW in {col}: max value = {max_val}")
                    if pd.notna(min_val) and min_val < -9223372036854775807:
                        logger.error(f"BIGINT UNDERFLOW in {col}: min value = {min_val}")
                    logger.info(f"{col}: range [{min_val}, {max_val}]")
                
                # Log conversion stats
                valid_count = df[col].notna().sum()
                logger.info(f"{col}: {valid_count}/{len(df)} valid ({valid_count/len(df)*100:.1f}%) - Sample before: {sample_before[:2]}")
        
        # Replace NaN/NaT with None for proper NULL handling in PostgreSQL
        # This must be done AFTER all type conversions
        logger.info("Replacing NaN/NaT with None for NULL handling")
        df = df.replace({pd.NaT: None})
        df = df.where(pd.notna(df), None)
        
        # Convert DataFrame to list of dictionaries
        # Only include columns that are in FIELD_MAPPING
        existing_cols = [col for col in FIELD_MAPPING.keys() if col in df.columns]
        logger.info(f"Columns to be synced: {existing_cols}")
        
        data = df[existing_cols].to_dict('records')
        
        logger.info(f"Transformed {len(data)} records")
        logger.info(f"Sample record: {data[0] if data else 'No data'}")
        
        return data
        
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise


def sync_data_to_postgres(conn, schema: str, table_name: str, data: List[Dict[str, Any]], batch_size: int):
    """Sync data to PostgreSQL in batches"""
    try:
        if not data:
            logger.warning("No data to sync")
            return
        
        logger.info(f"Starting sync of {len(data)} records to {schema}.{table_name}")
        
        # Get column names from first record
        columns = list(data[0].keys())
        
        # Prepare INSERT query
        insert_query = sql.SQL("""
            INSERT INTO {}.{} ({})
            VALUES ({})
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        
        # Prepare data tuples
        data_tuples = [tuple(record[col] for col in columns) for record in data]
        
        # Insert data in batches
        with conn.cursor() as cursor:
            total_batches = (len(data_tuples) + batch_size - 1) // batch_size
            total_inserted = 0
            
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                try:
                    logger.info(f"Inserting batch {batch_num}/{total_batches} ({len(batch)} records)")
                    
                    execute_batch(cursor, insert_query, batch, page_size=batch_size)
                    conn.commit()
                    
                    total_inserted += len(batch)
                    logger.info(f"Progress: {total_inserted}/{len(data_tuples)} records inserted ({total_inserted/len(data_tuples)*100:.1f}%)")
                    
                except Exception as batch_error:
                    logger.error(f"Error in batch {batch_num}: {batch_error}")
                    logger.error(f"Batch range: records {i} to {i + len(batch)}")
                    conn.rollback()
                    raise
        
        logger.info(f"Successfully synced {total_inserted} records to PostgreSQL")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error syncing data to PostgreSQL: {e}")
        raise


def truncate_table(conn, schema: str, table_name: str):
    """Truncate table before inserting new data"""
    try:
        with conn.cursor() as cursor:
            truncate_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table_name)
            )
            cursor.execute(truncate_query)
            conn.commit()
            logger.info(f"Table {schema}.{table_name} truncated successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error truncating table: {e}")
        raise


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Sync credit history data from CSV/Excel to PostgreSQL')
    parser.add_argument('--file', type=str, default=FILE_PATH, help='Path to the input file')
    parser.add_argument('--truncate', action='store_true', help='Truncate table before inserting')
    parser.add_argument('--batch-size', type=int, default=POSTGRES_TABLE_CONFIGS['batch_size'], 
                       help='Batch size for inserts')
    
    args = parser.parse_args()
    
    try:
        start_time = time.time()
        
        # Read data from file
        df = read_data_from_file(args.file)
        
        # Clean and transform data
        data = clean_and_transform_data(df)
        
        # Connect to PostgreSQL
        conn = get_postgres_client()
        
        schema = POSTGRES_SCHEMA
        table_name = POSTGRES_TABLE_CONFIGS["target_table_name"]
        
        # Check if table exists, create if not
        if not check_postgres_table_exists(conn, schema, table_name):
            logger.info(f"Table does not exist. Creating {schema}.{table_name}")
            create_postgres_table(conn, schema, table_name)
        
        # Truncate table if requested
        if args.truncate:
            truncate_table(conn, schema, table_name)
        
        # Sync data
        sync_data_to_postgres(conn, schema, table_name, data, args.batch_size)
        
        # Close connection
        conn.close()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Sync completed successfully in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise


if __name__ == "__main__":
    main()