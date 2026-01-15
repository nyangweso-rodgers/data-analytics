import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from typing import Dict, List, Set, Tuple
import openpyxl
from datetime import datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging

# ────────────────────────────────────────────────
# Logging & Env
# ────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# Global Configuration
DB_CONFIGS = {
    "amtdb_replica": {
        "host": os.getenv("SC_MYSQL_AMTDB_v39_REPLICA_HOST"),
        "user": os.getenv("SC_MYSQL_AMTDB_v39_REPLICA_USER"),
        "password": os.getenv("SC_MYSQL_AMTDB_v39_REPLICA_PASSWORD"),
        "database": os.getenv("SC_MYSQL_AMTDB_v39_REPLICA_DB"),
        "port": int(os.getenv("SC_MYSQL_AMTDB_v39_REPLICA_PORT", "3306")),
    }
}

DB_TABLE_CONFIGS = {
    "parent_table_name": "company_regions",
    "parent_id_column": "id",  # The column name in parent table
    "parent_id_value": 2,      # The specific region/country ID
}


class RegionalDataExtractor:
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.connection = None
        self.engine = None
        
    def connect(self):
        """Establish database connection"""
        try:
            # Create mysql.connector connection
            self.connection = mysql.connector.connect(**self.db_config)
            
            # Create SQLAlchemy engine for pandas (removes warnings)
            connection_string = (
                f"mysql+mysqlconnector://{self.db_config['user']}:"
                f"{quote_plus(self.db_config['password'])}@"
                f"{self.db_config['host']}:{self.db_config['port']}/"
                f"{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            
            print(f"✓ Connected to MySQL database: {self.db_config['database']}")
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        print("✓ Database connection closed")
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get total row count for a table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Error:
            return 0
    
    def is_real_table(self, table_name: str) -> bool:
        """
        Check if a table name represents a real table (not a view or stored procedure).
        Returns True only for BASE TABLEs.
        """
        query = """
        SELECT TABLE_TYPE 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = %s
        """
        
        cursor = self.connection.cursor()
        cursor.execute(query, (self.db_config['database'], table_name))
        result = cursor.fetchone()
        cursor.close()
        
        if result and result[0] == 'BASE TABLE':
            return True
        
        # If it's a VIEW, skip it
        if result and result[0] == 'VIEW':
            print(f"  ⊗ Skipping {table_name} (VIEW)")
            return False
        
        return False
        """Get total row count for a table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Error:
            return 0
    
    def get_child_tables(self, parent_table: str) -> List[Dict]:
        """
        Get all REAL tables (not views) that reference the parent table (child tables).
        These are tables with foreign keys pointing TO the parent table.
        """
        query = """
        SELECT 
            kcu.TABLE_NAME as child_table,
            kcu.COLUMN_NAME as child_column,
            kcu.REFERENCED_TABLE_NAME as parent_table,
            kcu.REFERENCED_COLUMN_NAME as parent_column,
            kcu.CONSTRAINT_NAME,
            t.TABLE_TYPE
        FROM 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN 
            INFORMATION_SCHEMA.TABLES t 
            ON kcu.TABLE_SCHEMA = t.TABLE_SCHEMA 
            AND kcu.TABLE_NAME = t.TABLE_NAME
        WHERE 
            kcu.REFERENCED_TABLE_SCHEMA = %s
            AND kcu.REFERENCED_TABLE_NAME = %s
            AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY kcu.TABLE_NAME
        """
        
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query, (self.db_config['database'], parent_table))
        child_relationships = cursor.fetchall()
        cursor.close()
        
        return child_relationships
    
    def get_all_descendant_tables(self, parent_table: str) -> Dict[str, List[Dict]]:
        """
        Recursively find all tables that depend on the parent table.
        Returns a map of table relationships in a tree structure.
        """
        visited = set()
        relationships_map = {}
        
        def explore_children(table: str, level: int = 0):
            if table in visited:
                return
            
            visited.add(table)
            children = self.get_child_tables(table)
            
            if children:
                relationships_map[table] = children
                print(f"{'  ' * level}└─ {table} has {len(children)} child table(s)")
                
                # Recursively explore each child
                for child_rel in children:
                    child_table = child_rel['child_table']
                    explore_children(child_table, level + 1)
            else:
                print(f"{'  ' * level}└─ {table} (leaf node)")
        
        print(f"\nDiscovering table hierarchy starting from '{parent_table}':")
        explore_children(parent_table)
        
        return relationships_map
    
    def fetch_table_data(self, table_name: str, where_clause: str = None, 
                         where_params: Tuple = None) -> pd.DataFrame:
        """Fetch data from a specific table with optional WHERE clause"""
        query = f"SELECT * FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        try:
            # Use SQLAlchemy engine to avoid pandas warning
            if where_params:
                df = pd.read_sql(query, self.engine, params=where_params)
            else:
                df = pd.read_sql(query, self.engine)
            
            total_rows = self.get_table_row_count(table_name)
            print(f"  ✓ {table_name}: {len(df):,} rows extracted (of {total_rows:,} total)")
            return df
        except Error as e:
            print(f"  ✗ Error fetching data from {table_name}: {e}")
            return pd.DataFrame()
    
    def extract_regional_data(self, parent_table: str, parent_id_column: str, 
                             parent_id_value: int) -> Dict[str, pd.DataFrame]:
        """
        Extract all data for a specific region/country.
        
        Process:
        1. Get the parent record (e.g., company_regions WHERE id = 2)
        2. Find all child tables that reference this parent
        3. For each child, extract rows where FK matches parent ID
        4. Recursively do the same for grandchildren, etc.
        
        Args:
            parent_table: Starting table (e.g., 'company_regions')
            parent_id_column: Column name for filtering (e.g., 'id')
            parent_id_value: The specific ID to extract (e.g., 2)
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        print(f"\n{'='*60}")
        print(f"EXTRACTING DATA FOR: {parent_table}.{parent_id_column} = {parent_id_value}")
        print(f"{'='*60}")
        
        # Get all descendant tables
        relationships_map = self.get_all_descendant_tables(parent_table)
        
        # Dictionary to store all extracted data
        data_frames = {}
        
        # Step 1: Extract parent table data
        print(f"\n--- Extracting Parent Table ---")
        parent_where = f"{parent_id_column} = %s"
        parent_df = self.fetch_table_data(parent_table, parent_where, (parent_id_value,))
        
        if parent_df.empty:
            print(f"⚠ No data found for {parent_table}.{parent_id_column} = {parent_id_value}")
            return data_frames
        
        data_frames[parent_table] = parent_df
        
        # Step 2: Process all child tables recursively
        print(f"\n--- Extracting Child Tables ---")
        processed = {parent_table}
        to_process = [(parent_table, parent_df, parent_id_column)]
        
        while to_process:
            current_table, current_df, current_key_column = to_process.pop(0)
            
            # Get children of current table
            if current_table not in relationships_map:
                continue
            
            for child_rel in relationships_map[current_table]:
                child_table = child_rel['child_table']
                child_fk_column = child_rel['child_column']      # FK in child table
                parent_pk_column = child_rel['parent_column']    # PK in parent table
                
                if child_table in processed:
                    continue
                
                # Skip if not a real table (e.g., it's a view)
                if not self.is_real_table(child_table):
                    processed.add(child_table)
                    continue
                
                # Extract values from parent table's PK column
                if parent_pk_column in current_df.columns:
                    parent_values = current_df[parent_pk_column].dropna().unique().tolist()
                    
                    if parent_values:
                        # Build WHERE clause for child table
                        placeholders = ','.join(['%s'] * len(parent_values))
                        where_clause = f"{child_fk_column} IN ({placeholders})"
                        
                        child_df = self.fetch_table_data(
                            child_table, 
                            where_clause, 
                            tuple(parent_values)
                        )
                        
                        if not child_df.empty:
                            data_frames[child_table] = child_df
                            # Add to queue to process its children
                            to_process.append((child_table, child_df, child_fk_column))
                
                processed.add(child_table)
        
        print(f"\n{'='*60}")
        print(f"EXTRACTION COMPLETE")
        print(f"Total tables extracted: {len(data_frames)}")
        print(f"Total rows extracted: {sum(len(df) for df in data_frames.values()):,}")
        print(f"{'='*60}")
        
        return data_frames
    
    def export_to_excel(self, data_frames: Dict[str, pd.DataFrame], 
                       output_file: str = None) -> str:
        """Export data to Excel with summary sheet"""
        if not data_frames:
            print("⚠ No data to export")
            return None
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"regional_data_export_{timestamp}.xlsx"
        
        print(f"\n--- Exporting to Excel: {output_file} ---")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Create summary sheet
            summary_data = []
            for table_name, df in sorted(data_frames.items()):
                summary_data.append({
                    'Table Name': table_name,
                    'Row Count': len(df),
                    'Column Count': len(df.columns)
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='_Summary', index=False)
            print(f"  ✓ Created summary sheet")
            
            # Export each table
            for table_name, df in sorted(data_frames.items()):
                sheet_name = table_name[:31]  # Excel limit
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"  ✓ {table_name} → '{sheet_name}' ({len(df):,} rows)")
        
        print(f"\n✓ Data exported successfully to: {output_file}")
        return output_file
    
    def get_extraction_stats(self, data_frames: Dict[str, pd.DataFrame]) -> Dict:
        """Get statistics about the extraction"""
        stats = {
            'total_tables': len(data_frames),
            'total_rows': sum(len(df) for df in data_frames.values()),
            'tables': []
        }
        
        for table_name, df in sorted(data_frames.items()):
            stats['tables'].append({
                'name': table_name,
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
            })
        
        return stats


def main():
    """Main execution function"""
    
    # Initialize extractor
    extractor = RegionalDataExtractor(DB_CONFIGS["amtdb_replica"])
    
    try:
        # Connect to database
        extractor.connect()
        
        # Extract data for the specified region
        data_frames = extractor.extract_regional_data(
            parent_table=DB_TABLE_CONFIGS["parent_table_name"],
            parent_id_column=DB_TABLE_CONFIGS["parent_id_column"],
            parent_id_value=DB_TABLE_CONFIGS["parent_id_value"]
        )
        
        if data_frames:
            # Get and display statistics
            stats = extractor.get_extraction_stats(data_frames)
            print(f"\n--- Extraction Statistics ---")
            print(f"Tables extracted: {stats['total_tables']}")
            print(f"Total rows: {stats['total_rows']:,}")
            print(f"Estimated size: {sum(t['size_mb'] for t in stats['tables']):.2f} MB")
            
            # Export to Excel
            output_file = f"region_{DB_TABLE_CONFIGS['parent_id_value']}_data_export.xlsx"
            extractor.export_to_excel(data_frames, output_file)
        else:
            print("\n⚠ No data extracted")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always disconnect
        extractor.disconnect()


if __name__ == "__main__":
    main()