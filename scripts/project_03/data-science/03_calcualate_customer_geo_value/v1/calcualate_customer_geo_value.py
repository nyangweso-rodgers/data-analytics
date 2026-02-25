import clickhouse_connect
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import geopandas as gpd
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ClickHouse Configuration
CLICKHOUSE_CONFIG = {
    "host": os.getenv("SC_CH_DB_HOST"),
    "port": int(os.getenv("SC_CH_DB_PORT", "9440")),
    "username": os.getenv("SC_CH_DB_USER"),
    "password": os.getenv("SC_CH_DB_PASSWORD"),
    "database": "credit_score_model",
    "secure": True,
    "verify": True,
}

CLICKHOUSE_TABLE_CONFIG = {
    "table_name": "cds",
    "table_columns": {
        "customerId": "String",
        "latitude": "Float64",
        "longitude": "Float64"
    },
}

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "host": os.getenv("SC_REPORTING_SERVICE_PG_DB_HOST"),
    "port": int(os.getenv("SC_REPORTING_SERVICE_PG_DB_PORT", "5432")),
    "user": os.getenv("SC_REPORTING_SERVICE_PG_DB_USER"),
    "password": os.getenv("SC_REPORTING_SERVICE_PG_DB_PASSWORD"),
    "database": "reporting-service",
}

# PostgreSQL Table Configurations
POSTGRES_TABLE_CONFIGS = {
    "schema": "data_science",
    "table_name": "customer_geo_value",
    "primary_key": ["customerId"],  # Primary key columns
    "indexes": [
        ["customerId"], 
    ],
    "unique_constraints": [],
    "batch_size": 5000,  # Batch size for inserts
}

GEOJSON_FILE_PATH = "../../../../../../../ASU expansion prediction layer.geojson"
OUTPUT_DIR = "output"  # Directory to store generated files

# Kenya bounding box
KENYA_MIN_LON, KENYA_MAX_LON = 34.0, 42.0
KENYA_MIN_LAT, KENYA_MAX_LAT = -5.0, 5.0


def ensure_output_directory():
    """Create output directory if it doesn't exist"""
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {OUTPUT_DIR}")


def generate_timestamped_filename(base_name: str = "customer_geo_value", extension: str = "xlsx") -> str:
    """Generate timestamped filename"""
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    filename = f"{base_name}-{timestamp}.{extension}"
    filepath = os.path.join(OUTPUT_DIR, filename)
    return filepath


def get_latest_excel_file(base_name: str = "customer_geo_value") -> Optional[str]:
    """Get the most recent Excel file matching the pattern"""
    try:
        output_path = Path(OUTPUT_DIR)
        if not output_path.exists():
            return None
        
        # Find all matching files
        pattern = f"{base_name}-*.xlsx"
        files = list(output_path.glob(pattern))
        
        if not files:
            return None
        
        # Sort by modification time and get the latest
        latest_file = max(files, key=lambda x: x.stat().st_mtime)
        logger.info(f"Found latest Excel file: {latest_file}")
        return str(latest_file)
    except Exception as e:
        logger.error(f"Error finding latest Excel file: {e}")
        return None


def get_clickhouse_client():
    """Establish connection to ClickHouse"""
    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        logger.info("Successfully connected to ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise

    
def get_postgres_client():
    """Establish connection to PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        logger.info("Successfully connected to PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def load_geojson_data(file_path: str) -> gpd.GeoDataFrame:
    """Load and filter GeoJSON data for Kenya region"""
    try:
        logger.info(f"Loading GeoJSON file from: {file_path}")
        geo_data = gpd.read_file(file_path)
        
        logger.info(f"Original GeoJSON size: {geo_data.shape}")
        logger.info(f"CRS: {geo_data.crs}")
        logger.info(f"Total bounds: {geo_data.total_bounds}")
        
        # Filter for Kenya bounding box
        geo_data_kenya = geo_data.cx[KENYA_MIN_LON:KENYA_MAX_LON, KENYA_MIN_LAT:KENYA_MAX_LAT]
        logger.info(f"Kenya-filtered size: {geo_data_kenya.shape}")
        
        return geo_data_kenya
    except Exception as e:
        logger.error(f"Failed to load GeoJSON file: {e}")
        raise


def fetch_customer_locations(client) -> pd.DataFrame:
    """Fetch customer location data from ClickHouse"""
    try:
        table_name = CLICKHOUSE_TABLE_CONFIG["table_name"]
        query = f"""
        SELECT 
            customerId,
            longitude,
            latitude
        FROM {table_name}
        WHERE latitude IS NOT NULL 
          AND longitude IS NOT NULL
        """
        
        logger.info(f"Fetching customer data from table: {table_name}")
        result = client.query(query)
        
        location_data = pd.DataFrame(result.result_rows, columns=result.column_names)
        logger.info(f"Fetched {len(location_data)} customer records")
        
        # Add missing indicators
        location_data['latitude_missing'] = 0
        location_data['longitude_missing'] = 0
        
        return location_data
    except Exception as e:
        logger.error(f"Failed to fetch customer data: {e}")
        raise


def calculate_geo_values(location_data: pd.DataFrame, geo_data_kenya: gpd.GeoDataFrame) -> pd.DataFrame:
    """Calculate geo values for customer locations using spatial join"""
    try:
        logger.info("Converting customer locations to GeoDataFrame")
        location_gdf = gpd.GeoDataFrame(
            location_data,
            geometry=gpd.points_from_xy(location_data["longitude"], location_data["latitude"]),
            crs="EPSG:4326"
        )
        
        logger.info("Performing spatial join with geo data")
        joined_geo = gpd.sjoin(
            location_gdf,
            geo_data_kenya,
            how="left",
            predicate="within"
        )
        
        # Rename VALUE column to geo_value
        joined_geo = joined_geo.rename(columns={"VALUE": "geo_value"})
        
        # Select final columns
        location_final = joined_geo[[
            "customerId",
            "longitude",
            "latitude",
            "latitude_missing",
            "longitude_missing",
            "geo_value"
        ]].copy()
        
        # Log missing rate
        missing_rate = location_final["geo_value"].isna().mean()
        logger.info(f"Geo value missing rate: {missing_rate:.2%}")
        
        # Add missing indicator
        location_final.loc[:, "geo_value_missing"] = location_final["geo_value"].isna().astype(int)
        
        # Median impute
        median_value = location_final["geo_value"].median()
        logger.info(f"Imputing missing geo values with median: {median_value}")
        location_final.loc[:, "geo_value"] = location_final["geo_value"].fillna(median_value)
        
        return location_final
    except Exception as e:
        logger.error(f"Failed to calculate geo values: {e}")
        raise


def export_to_excel(data: pd.DataFrame) -> str:
    """Export customer ID and geo value to Excel file with timestamp"""
    try:
        # Generate timestamped filename
        output_path = generate_timestamped_filename()
        
        # Select only customerId and geo_value columns
        export_data = data[["customerId", "geo_value"]].copy()
        
        logger.info(f"Exporting {len(export_data)} records to Excel: {output_path}")
        export_data.to_excel(output_path, index=False, engine='openpyxl')
        
        logger.info(f"Successfully exported data to {output_path}")
        logger.info(f"File size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")
        
        return output_path
    except Exception as e:
        logger.error(f"Failed to export to Excel: {e}")
        raise


def read_excel_file(file_path: str) -> pd.DataFrame:
    """Read Excel file into DataFrame"""
    try:
        logger.info(f"Reading Excel file: {file_path}")
        df = pd.read_excel(file_path, engine='openpyxl')
        logger.info(f"Loaded {len(df)} records from Excel")
        logger.info(f"Columns: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        raise


def check_postgres_schema_exists(conn, schema: str) -> bool:
    """Check if schema exists in PostgreSQL"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata 
                    WHERE schema_name = %s
                )
            """, (schema,))
            exists = cur.fetchone()[0]
            
            if exists:
                logger.info(f"PostgreSQL schema '{schema}' exists")
            else:
                logger.info(f"PostgreSQL schema '{schema}' does not exist")
            
            return exists
    except Exception as e:
        logger.error(f"Error checking PostgreSQL schema existence: {e}")
        return False


def create_postgres_schema(conn, schema: str):
    """Create schema in PostgreSQL if it doesn't exist"""
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            conn.commit()
            logger.info(f"Created or verified schema: {schema}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create schema: {e}")
        raise


def check_postgres_table_exists(conn, schema: str, table_name: str) -> bool:
    """Check if table exists in PostgreSQL"""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """, (schema, table_name))
            exists = cur.fetchone()[0]
            
            if exists:
                logger.info(f"PostgreSQL table '{schema}.{table_name}' exists")
            else:
                logger.info(f"PostgreSQL table '{schema}.{table_name}' does not exist")
            
            return exists
    except Exception as e:
        logger.error(f"Error checking PostgreSQL table existence: {e}")
        return False


def create_postgres_table(conn, schema: str, table_name: str, table_config: Dict[str, Any]):
    """Create PostgreSQL table with proper schema"""
    try:
        with conn.cursor() as cur:
            # Define table columns based on expected data
            # Using lowercase column names for PostgreSQL convention
            create_table_sql = sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    customer_id VARCHAR(255) NOT NULL,
                    geo_value DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(
                sql.Identifier(schema),
                sql.Identifier(table_name)
            )
            
            cur.execute(create_table_sql)
            logger.info(f"Created table: {schema}.{table_name}")
            
            # Create primary key if specified
            if table_config.get("primary_key"):
                # Map camelCase to snake_case for PostgreSQL
                pk_columns = table_config["primary_key"]
                pk_columns_mapped = ["customer_id" if col == "customerId" else col for col in pk_columns]
                pk_name = f"{table_name}_pkey"
                
                # Check if primary key already exists
                cur.execute("""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_schema = %s 
                    AND table_name = %s 
                    AND constraint_type = 'PRIMARY KEY'
                """, (schema, table_name))
                
                if not cur.fetchone():
                    pk_sql = sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY ({})").format(
                        sql.Identifier(schema),
                        sql.Identifier(table_name),
                        sql.Identifier(pk_name),
                        sql.SQL(", ").join(map(sql.Identifier, pk_columns_mapped))
                    )
                    cur.execute(pk_sql)
                    logger.info(f"Created primary key on: {', '.join(pk_columns_mapped)}")
            
            # Create indexes if specified
            if table_config.get("indexes"):
                for idx, columns in enumerate(table_config["indexes"]):
                    # Map camelCase to snake_case for PostgreSQL
                    columns_mapped = ["customer_id" if col == "customerId" else col for col in columns]
                    idx_name = f"{table_name}_{'_'.join(columns_mapped)}_idx"
                    
                    # Check if index already exists
                    cur.execute("""
                        SELECT indexname 
                        FROM pg_indexes 
                        WHERE schemaname = %s 
                        AND tablename = %s 
                        AND indexname = %s
                    """, (schema, table_name, idx_name))
                    
                    if not cur.fetchone():
                        idx_sql = sql.SQL("CREATE INDEX {} ON {}.{} ({})").format(
                            sql.Identifier(idx_name),
                            sql.Identifier(schema),
                            sql.Identifier(table_name),
                            sql.SQL(", ").join(map(sql.Identifier, columns_mapped))
                        )
                        cur.execute(idx_sql)
                        logger.info(f"Created index: {idx_name} on {', '.join(columns_mapped)}")
            
            conn.commit()
            logger.info(f"Table {schema}.{table_name} created successfully with constraints")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create PostgreSQL table: {e}")
        raise


def sync_data_to_postgres(conn, df: pd.DataFrame, schema: str, table_name: str, batch_size: int = 5000):
    """Sync data to PostgreSQL using UPSERT (INSERT ... ON CONFLICT UPDATE)"""
    try:
        total_records = len(df)
        logger.info(f"Starting sync of {total_records} records to {schema}.{table_name}")
        
        # Rename columns to match PostgreSQL snake_case convention
        df_sync = df.copy()
        df_sync = df_sync.rename(columns={"customerId": "customer_id"})
        
        # Prepare data for insertion
        records = df_sync[["customer_id", "geo_value"]].to_records(index=False).tolist()
        
        # Upsert query with ON CONFLICT
        upsert_sql = sql.SQL("""
            INSERT INTO {}.{} (customer_id, geo_value, created_at, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (customer_id) 
            DO UPDATE SET 
                geo_value = EXCLUDED.geo_value,
                updated_at = CURRENT_TIMESTAMP
        """).format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        )
        
        with conn.cursor() as cur:
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                execute_batch(cur, upsert_sql, batch, page_size=batch_size)
                conn.commit()
                
                synced = min(i + batch_size, total_records)
                logger.info(f"Synced {synced}/{total_records} records ({synced/total_records*100:.1f}%)")
        
        logger.info(f"Successfully synced all {total_records} records to PostgreSQL")
        
        # Verify record count
        with conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table_name)
            ))
            count = cur.fetchone()[0]
            logger.info(f"Total records in {schema}.{table_name}: {count}")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to sync data to PostgreSQL: {e}")
        raise

def calculate_task():
    """Task: Calculate geo values and export to Excel"""
    try:
        ensure_output_directory()
        
        # Step 1: Load GeoJSON data
        logger.info("=" * 50)
        logger.info("Step 1: Loading GeoJSON data")
        logger.info("=" * 50)
        geo_data_kenya = load_geojson_data(GEOJSON_FILE_PATH)
        
        # Step 2: Fetch customer locations from ClickHouse
        logger.info("=" * 50)
        logger.info("Step 2: Fetching customer data from ClickHouse")
        logger.info("=" * 50)
        client = get_clickhouse_client()
        location_data = fetch_customer_locations(client)
        
        # Step 3: Calculate geo values
        logger.info("=" * 50)
        logger.info("Step 3: Calculating geo values")
        logger.info("=" * 50)
        location_final = calculate_geo_values(location_data, geo_data_kenya)
        
        # Step 4: Export to Excel
        logger.info("=" * 50)
        logger.info("Step 4: Exporting to Excel")
        logger.info("=" * 50)
        output_file = export_to_excel(location_final)
        
        logger.info("=" * 50)
        logger.info(f"✓ Calculate task completed successfully!")
        logger.info(f"✓ Output file: {output_file}")
        logger.info("=" * 50)
        
        client.close()
        logger.info("ClickHouse connection closed")
        
    except Exception as e:
        logger.error(f"Calculate task failed: {e}")
        raise


def sync_to_postgres_task(file_path: Optional[str] = None):
    """Task: Sync Excel data to PostgreSQL"""
    try:
        # Step 1: Determine which file to sync
        if file_path:
            logger.info(f"Using specified file: {file_path}")
            excel_file = file_path
        else:
            logger.info("Looking for latest Excel file...")
            excel_file = get_latest_excel_file()
            if not excel_file:
                raise FileNotFoundError(
                    f"No Excel files found in {OUTPUT_DIR}. "
                    "Please run the calculate task first or specify a file path."
                )
        
        # Step 2: Read Excel file
        logger.info("=" * 50)
        logger.info("Step 1: Reading Excel file")
        logger.info("=" * 50)
        df = read_excel_file(excel_file)
        
        # Step 3: Connect to PostgreSQL
        logger.info("=" * 50)
        logger.info("Step 2: Connecting to PostgreSQL")
        logger.info("=" * 50)
        conn = get_postgres_client()
        
        # Step 4: Ensure schema exists
        schema = POSTGRES_TABLE_CONFIGS["schema"]
        if not check_postgres_schema_exists(conn, schema):
            create_postgres_schema(conn, schema)
        
        # Step 5: Create table if not exists
        logger.info("=" * 50)
        logger.info("Step 3: Creating/verifying PostgreSQL table")
        logger.info("=" * 50)
        table_name = POSTGRES_TABLE_CONFIGS["table_name"]
        if not check_postgres_table_exists(conn, schema, table_name):
            create_postgres_table(conn, schema, table_name, POSTGRES_TABLE_CONFIGS)
        
        # Step 6: Sync data
        logger.info("=" * 50)
        logger.info("Step 4: Syncing data to PostgreSQL")
        logger.info("=" * 50)
        sync_data_to_postgres(
            conn, 
            df, 
            schema, 
            table_name, 
            POSTGRES_TABLE_CONFIGS["batch_size"]
        )
        
        logger.info("=" * 50)
        logger.info(f"✓ Sync to PostgreSQL completed successfully!")
        logger.info(f"✓ Table: {schema}.{table_name}")
        logger.info("=" * 50)
        
        conn.close()
        logger.info("PostgreSQL connection closed")
        
    except Exception as e:
        logger.error(f"Sync to PostgreSQL task failed: {e}")
        raise


def main():
    """Main entry point with command-line argument parsing"""
    parser = argparse.ArgumentParser(
        description="Calculate customer geo values and sync to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calculate geo values and export to Excel
  python calculate_customer_geo_value.py
  
  # Sync latest Excel file to PostgreSQL
  python calculate_customer_geo_value.py --task sync_to_postgres
  
  # Sync specific Excel file to PostgreSQL
  python calculate_customer_geo_value.py --task sync_to_postgres --file output/customer_geo_value-2025_02_25_13_53.xlsx
  
  # Calculate and immediately sync
  python calculate_customer_geo_value.py --task calculate_and_sync
        """
    )
    
    parser.add_argument(
        '--task',
        type=str,
        choices=['calculate', 'sync_to_postgres', 'calculate_and_sync'],
        default='calculate',
        help='Task to execute (default: calculate)'
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Path to Excel file for sync_to_postgres task (optional, uses latest if not specified)'
    )
    
    args = parser.parse_args()
    
    try:
        if args.task == 'calculate':
            calculate_task()
            
        elif args.task == 'sync_to_postgres':
            sync_to_postgres_task(args.file)
            
        elif args.task == 'calculate_and_sync':
            # First calculate
            calculate_task()
            # Then sync the file that was just created
            latest_file = get_latest_excel_file()
            if latest_file:
                sync_to_postgres_task(latest_file)
            else:
                raise FileNotFoundError("No Excel file found after calculation")
        
    except Exception as e:
        logger.error(f"Task execution failed: {e}")
        raise


if __name__ == "__main__":
    main()