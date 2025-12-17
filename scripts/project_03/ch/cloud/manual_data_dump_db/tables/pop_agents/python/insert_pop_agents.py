import clickhouse_connect
import os
from dotenv import load_dotenv
import argparse
import csv
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

# Load environment variables
load_dotenv()


# ============================================================================
# CONFIGURATION
# ============================================================================
CH_CONFIGS = {
    "host": os.getenv("SC_CH_DB_HOST"),
    "port": int(os.getenv("SC_CH_DB_PORT", "8443")),
    "username": os.getenv("SC_CH_DB_USER"),
    "password": os.getenv("SC_CH_DB_PASSWORD"),
    "database": "manual_data_dump", 
    "table_name": "pop_agents_v2",
}

CH_TABLE_CONFIG = {
    "table_name": "pop_agents_v2",
    "columns": [
        ("amt_id", "Int32"),
        ("amt_identification_number", "String"),
        ("amt_phone_number", "String"),
        ("amt_name", "String"),
        ("amt_department", "String"),
        ("amt_status", "String"),
        ("agent_category", "String"),
        ("amt_supervisor_id", "Int32"),
        ("amt_supervisor_name", "String"),
    ],
    "engine": "MergeTree()",
    "order_by": "amt_id",
    "unique_fields": ["amt_phone_number", "amt_identification_number"]  # Fields that must be unique
}

# Expected CSV columns
EXPECTED_CSV_COLUMNS = [
    "amt_id",
    "amt_identification_number",
    "amt_phone_number",
    "amt_name",
    "amt_department",
    "amt_status",
    "agent_category",
    "amt_supervisor_id",
    "amt_supervisor_name"
]

# Default CSV file path
DEFAULT_CSV_FILE = "../../../../../../../../../../upload pop agents - 2025-12-17.csv"


# ============================================================================
# ClickHouse Client
# ============================================================================
def get_client():
    """Get ClickHouse client using CH_CONFIGS"""
    return clickhouse_connect.get_client(
        host=CH_CONFIGS["host"],
        port=CH_CONFIGS["port"],
        username=CH_CONFIGS["username"],
        password=CH_CONFIGS["password"],
    )


def validate_clickhouse_db_table() -> Tuple[bool, bool]:
    """
    Validate if database and table exist
    Returns: (database_exists, table_exists)
    """
    client = get_client()
    db_name = CH_CONFIGS["database"]
    table_name = CH_CONFIGS["table_name"]
    
    # Check if database exists
    db_query = f"SELECT count() FROM system.databases WHERE name = '{db_name}'"
    db_exists = client.command(db_query) > 0
    
    # Check if table exists (only if database exists)
    table_exists = False
    if db_exists:
        table_query = f"SELECT count() FROM system.tables WHERE database = '{db_name}' AND name = '{table_name}'"
        table_exists = client.command(table_query) > 0
    
    return db_exists, table_exists


def create_clickhouse_db_table():
    """Create ClickHouse database and table if they do not exist"""
    client = get_client()
    db_name = CH_CONFIGS["database"]
    table_name = CH_CONFIGS["table_name"]
    
    db_exists, table_exists = validate_clickhouse_db_table()
    
    # Create database if it doesn't exist
    if not db_exists:
        print(f"Creating database: {db_name}")
        client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"✓ Database '{db_name}' created successfully")
    else:
        print(f"✓ Database '{db_name}' already exists")
    
    # Create table if it doesn't exist
    if not table_exists:
        print(f"Creating table: {table_name}")
        
        # Build column definitions
        columns_def = ", ".join([f"{col[0]} {col[1]}" for col in CH_TABLE_CONFIG["columns"]])
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
            {columns_def}
        )
        ENGINE = {CH_TABLE_CONFIG["engine"]}
        ORDER BY {CH_TABLE_CONFIG["order_by"]}
        """
        
        client.command(create_table_query)
        print(f"✓ Table '{table_name}' created successfully")
    else:
        print(f"✓ Table '{table_name}' already exists")


def read_csv_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Read CSV file and return list of dictionaries
    Args:
        file_path: Path to CSV file
    Returns:
        List of dictionaries containing row data
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Validate CSV columns
        csv_columns = reader.fieldnames
        if not csv_columns:
            raise ValueError("CSV file is empty or has no headers")
        
        # Check for missing columns
        missing_cols = set(EXPECTED_CSV_COLUMNS) - set(csv_columns)
        if missing_cols:
            raise ValueError(f"CSV is missing required columns: {missing_cols}")
        
        # Check for extra columns (warning only)
        extra_cols = set(csv_columns) - set(EXPECTED_CSV_COLUMNS)
        if extra_cols:
            print(f"⚠ Warning: CSV contains extra columns that will be ignored: {extra_cols}")
        
        # Read rows
        for row_num, row in enumerate(reader, start=2):
            try:
                # Convert and validate data types - MATCH YOUR CSV COLUMNS
                processed_row = {
                    "amt_id": int(row["amt_id"]) if row["amt_id"].strip() else 0,
                    "amt_identification_number": str(row["amt_identification_number"]).strip(),
                    "amt_phone_number": str(row["amt_phone_number"]).strip(),
                    "amt_name": str(row["amt_name"]).strip(),
                    "amt_department": str(row["amt_department"]).strip(),
                    "amt_status": str(row["amt_status"]).strip(),
                    "agent_category": str(row["agent_category"]).strip(),
                    "amt_supervisor_id": int(row["amt_supervisor_id"]) if row["amt_supervisor_id"].strip() else 0,
                    "amt_supervisor_name": str(row["amt_supervisor_name"]).strip(),
                }
                data.append(processed_row)
            except (ValueError, KeyError) as e:
                print(f"⚠ Warning: Skipping row {row_num} due to error: {e}")
                print(f"   Row data: {row}")
                continue
    
    print(f"✓ Successfully read {len(data)} rows from CSV")
    return data


def get_existing_unique_values() -> Dict[str, set]:
    """
    Get existing unique values from the database
    Returns: Dictionary with field names as keys and sets of existing values
    """
    client = get_client()
    db_name = CH_CONFIGS["database"]
    table_name = CH_CONFIGS["table_name"]
    unique_fields = CH_TABLE_CONFIG.get("unique_fields", [])
    
    existing_values = {}
    
    # Check if table exists
    _, table_exists = validate_clickhouse_db_table()
    if not table_exists:
        # Return empty sets if table doesn't exist
        return {field: set() for field in unique_fields}
    
    # Query existing values for each unique field
    for field in unique_fields:
        query = f"SELECT DISTINCT {field} FROM {db_name}.{table_name} WHERE {field} != ''"
        result = client.query(query)
        existing_values[field] = set(row[0] for row in result.result_rows)
    
    return existing_values


def validate_unique_constraints(data: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Validate unique constraints for phone_number and id_number
    Args:
        data: List of dictionaries to validate
    Returns:
        Tuple of (valid_rows, invalid_rows)
    """
    unique_fields = CH_TABLE_CONFIG.get("unique_fields", [])
    
    # Get existing values from database
    existing_values = get_existing_unique_values()
    
    valid_rows = []
    invalid_rows = []
    
    # Track values seen in current batch
    batch_values = {field: set() for field in unique_fields}
    
    for row in data:
        is_valid = True
        duplicate_fields = []
        
        for field in unique_fields:
            value = row.get(field, "")
            
            # Check if value already exists in database
            if value in existing_values[field]:
                is_valid = False
                duplicate_fields.append(f"{field}={value} (exists in database)")
            
            # Check if value is duplicate within current batch
            elif value in batch_values[field]:
                is_valid = False
                duplicate_fields.append(f"{field}={value} (duplicate in CSV)")
            
            # Add to batch tracking
            batch_values[field].add(value)
        
        if is_valid:
            valid_rows.append(row)
        else:
            row['_duplicate_reason'] = ", ".join(duplicate_fields)
            invalid_rows.append(row)
    
    return valid_rows, invalid_rows


def insert_data_into_table(data: List[Dict[str, Any]], skip_validation: bool = False):
    """
    Insert data into ClickHouse table with unique constraint validation
    Args:
        data: List of dictionaries containing row data
        skip_validation: If True, skip unique constraint validation (not recommended)
    """
    if not data:
        print("No data to insert")
        return
    
    # Ensure database and table exist
    create_clickhouse_db_table()
    
    # Validate unique constraints unless skipped
    if not skip_validation:
        print("Validating unique constraints...")
        valid_rows, invalid_rows = validate_unique_constraints(data)
        
        if invalid_rows:
            print(f"\n⚠ Found {len(invalid_rows)} rows with duplicate values:")
            for row in invalid_rows[:10]:  # Show first 10
                print(f"  - Row ID {row.get('id')}: {row.get('_duplicate_reason')}")
            if len(invalid_rows) > 10:
                print(f"  ... and {len(invalid_rows) - 10} more")
            
            print(f"\n✓ {len(valid_rows)} rows passed validation")
            
            if not valid_rows:
                print("No valid rows to insert")
                return
        else:
            print(f"✓ All {len(data)} rows passed validation")
            valid_rows = data
    else:
        print("⚠ Skipping unique constraint validation")
        valid_rows = data
    
    # Insert valid rows
    client = get_client()
    db_name = CH_CONFIGS["database"]
    table_name = CH_CONFIGS["table_name"]
    
    # Prepare column names from the first record
    column_names = list(valid_rows[0].keys())
    if '_duplicate_reason' in column_names:
        column_names.remove('_duplicate_reason')
    
    # Prepare data as list of lists
    rows = [[row[col] for col in column_names] for row in valid_rows]
    
    try:
        # Insert data
        client.insert(
            f"{db_name}.{table_name}",
            rows,
            column_names=column_names
        )
        print(f"✓ Successfully inserted {len(valid_rows)} rows into {db_name}.{table_name}")
    except Exception as e:
        print(f"✗ Error inserting data: {e}")
        raise


def query_table(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Query data from the table
    Args:
        limit: Number of rows to return
    Returns:
        List of dictionaries containing row data
    """
    client = get_client()
    db_name = CH_CONFIGS["database"]
    table_name = CH_CONFIGS["table_name"]
    
    query = f"SELECT * FROM {db_name}.{table_name} LIMIT {limit}"
    result = client.query(query)
    
    # Convert to list of dictionaries
    columns = result.column_names
    rows = []
    for row in result.result_rows:
        rows.append(dict(zip(columns, row)))
    
    return rows


def count_rows() -> int:
    """Count total rows in the table"""
    client = get_client()
    db_name = CH_CONFIGS["database"]
    table_name = CH_CONFIGS["table_name"]
    
    query = f"SELECT count() FROM {db_name}.{table_name}"
    return client.command(query)


def main():
    """Main function to demonstrate script usage"""
    parser = argparse.ArgumentParser(description="ClickHouse Data Management Script")
    parser.add_argument(
        "--action",
        choices=["validate", "create", "insert", "query", "count"],
        default="insert",
        help="Action to perform"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=DEFAULT_CSV_FILE,
        help=f"Path to CSV file for insert action (default: {DEFAULT_CSV_FILE})"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip unique constraint validation (not recommended)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Limit for query results"
    )
    
    args = parser.parse_args()
    
    try:
        if args.action == "validate":
            db_exists, table_exists = validate_clickhouse_db_table()
            print(f"Database exists: {db_exists}")
            print(f"Table exists: {table_exists}")
        
        elif args.action == "create":
            create_clickhouse_db_table()
        
        elif args.action == "insert":
            # Use default file if not specified
            csv_file = args.file
            
            print(f"Reading data from: {csv_file}")
            data = read_csv_file(csv_file)
            
            print(f"Inserting {len(data)} rows...")
            insert_data_into_table(data, skip_validation=args.skip_validation)
            
            total = count_rows()
            print(f"Total rows in table: {total}")
        
        elif args.action == "query":
            rows = query_table(limit=args.limit)
            print(f"Retrieved {len(rows)} rows:")
            for row in rows:
                print(row)
        
        elif args.action == "count":
            total = count_rows()
            print(f"Total rows in table: {total}")
    
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()