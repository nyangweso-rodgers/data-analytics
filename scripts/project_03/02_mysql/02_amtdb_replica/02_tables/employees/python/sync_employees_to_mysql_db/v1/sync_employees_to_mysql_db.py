import os
import logging
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
from decimal import Decimal
from typing import Tuple, List, Dict, Any
import mysql.connector
from mysql.connector import Error
import argparse
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

MYSQL_CONFIGS = {
    "source_db":{
        "mysql_source_db_host": os.getenv("SC_AMT_REPLICA_MYSQL_DB_HOST"),
        "mysql_source_db_port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "mysql_source_db_name": os.getenv("SC_AMT_REPLICA_MYSQL_DB_NAME"),
        "mysql_source_db_user": os.getenv("SC_AMT_REPLICA_MYSQL_DB_USER"),
        "mysql_source_db_password": os.getenv("SC_AMT_REPLICA_MYSQL_DB_PASSWORD"),
        "mysql_source_table_name": "employees"
        },
    "target_db":{
        "mysql_target_db_host": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_HOST"),
        "mysql_target_db_port": int(os.getenv("MYSQL_DB_PORT", 3306)),
        "mysql_target_db_user": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_USER"),
        "mysql_target_db_password": os.getenv("SC_SALES_SERVICE_DEV_MYSQL_DB_PASSWORD"),
        "mysql_target_db_name": "data-migration-staging",
        "mysql_target_table_name": "stg_amt_employees"}
}

DB_FIELDS_LIST = ["id","countryId", "createdAt", "departmentId", "email", "identificationNumber", "isCustomer", "mobileMoneyPhoneNumber", "name", "phoneNumber", "primaryRoleId", "roleId", "salesForceAgentId","supervisorId"]

class DatabaseMigrator:
    def __init__(self, source_config: Dict[str, Any], target_config: Dict[str, Any], batch_size: int = 1000):
        self.source_config = source_config
        self.target_config = target_config
        self.batch_size = batch_size
        
    def create_db_connection(self, config: Dict[str, Any]) -> mysql.connector.connection.MySQLConnection:
        """Create database connection"""
        try:
            connection = mysql.connector.connect(
                host=config.get('mysql_source_db_host') or config.get('mysql_target_db_host'),
                port=config.get('mysql_source_db_port') or config.get('mysql_target_db_port'),
                database=config.get('mysql_source_db_name') or config.get('mysql_target_db_name'),
                user=config.get('mysql_source_db_user') or config.get('mysql_target_db_user'),
                password=config.get('mysql_source_db_password') or config.get('mysql_target_db_password')
            )
            logger.info(f"Successfully connected to database")
            return connection
        except Error as e:
            logger.error(f"Error connecting to MySQL database: {e}")
            raise

    def get_source_record_count(self) -> int:
        """Get total number of records in source table"""
        try:
            connection = self.create_db_connection(self.source_config)
            cursor = connection.cursor()
            
            query = f"SELECT COUNT(*) FROM {self.source_config['mysql_source_table_name']}"
            cursor.execute(query)
            count = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            logger.info(f"Total records in source table: {count}")
            return count
        except Error as e:
            logger.error(f"Error getting record count: {e}")
            raise

    def fetch_records_batch(self, offset: int) -> List[Tuple]:
        """Fetch a batch of records from source database"""
        try:
            connection = self.create_db_connection(self.source_config)
            cursor = connection.cursor()
            
            fields = ", ".join(DB_FIELDS_LIST)
            query = f"""
                SELECT {fields} 
                FROM {self.source_config['mysql_source_table_name']} 
                LIMIT %s OFFSET %s
            """
            
            cursor.execute(query, (self.batch_size, offset))
            records = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            logger.info(f"Fetched {len(records)} records from offset {offset}")
            return records
        except Error as e:
            logger.error(f"Error fetching records batch: {e}")
            raise

    def create_target_table_if_not_exists(self):
        """Create target table if it doesn't exist"""
        try:
            connection = self.create_db_connection(self.target_config)
            cursor = connection.cursor()
            
            # Create table with the same structure as source
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {self.target_config['mysql_target_table_name']} (
                id VARCHAR(255) PRIMARY KEY,
                countryId VARCHAR(255),
                createdAt DATETIME,
                departmentId VARCHAR(255),
                email VARCHAR(255),
                identificationNumber VARCHAR(255),
                isCustomer TINYINT(1),
                mobileMoneyPhoneNumber VARCHAR(255),
                name VARCHAR(255),
                phoneNumber VARCHAR(255),
                primaryRoleId VARCHAR(255),
                roleId VARCHAR(255),
                salesForceAgentId VARCHAR(255),
                supervisorId VARCHAR(255),
                migrated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            cursor.execute(create_table_query)
            connection.commit()
            
            cursor.close()
            connection.close()
            
            logger.info("Target table created/verified successfully")
        except Error as e:
            logger.error(f"Error creating target table: {e}")
            raise

    def insert_records_batch(self, records: List[Tuple]):
        """Insert a batch of records into target database"""
        if not records:
            return
            
        try:
            connection = self.create_db_connection(self.target_config)
            cursor = connection.cursor()
            
            # Prepare placeholders for the insert query
            placeholders = ", ".join(["%s"] * len(DB_FIELDS_LIST))
            fields = ", ".join(DB_FIELDS_LIST)
            
            insert_query = f"""
                INSERT INTO {self.target_config['mysql_target_table_name']} ({fields})
                VALUES ({placeholders})
            """
            
            cursor.executemany(insert_query, records)
            connection.commit()
            
            cursor.close()
            connection.close()
            
            logger.info(f"Successfully inserted {len(records)} records into target table")
        except Error as e:
            logger.error(f"Error inserting records batch: {e}")
            raise

    def migrate_data(self, start_offset: int = 0):
        """Main method to migrate data from source to target"""
        try:
            logger.info("Starting data migration process...")
            
            # Create target table if not exists
            self.create_target_table_if_not_exists()
            
            # Get total record count
            total_records = self.get_source_record_count()
            
            if total_records == 0:
                logger.warning("No records found in source table")
                return
            
            # Migrate data in batches
            migrated_count = 0
            offset = start_offset
            
            while offset < total_records:
                logger.info(f"Processing batch from offset {offset}")
                
                # Fetch records from source
                records = self.fetch_records_batch(offset)
                
                if not records:
                    break
                
                # Insert records into target
                self.insert_records_batch(records)
                
                migrated_count += len(records)
                offset += self.batch_size
                
                logger.info(f"Progress: {migrated_count}/{total_records} records migrated ({ (migrated_count/total_records)*100:.2f}%)")
            
            logger.info(f"Data migration completed successfully! Total records migrated: {migrated_count}")
            
        except Exception as e:
            logger.error(f"Data migration failed: {e}")
            raise

    def validate_migration(self) -> bool:
        """Validate that source and target have same record count"""
        try:
            source_count = self.get_source_record_count()
            
            connection = self.create_db_connection(self.target_config)
            cursor = connection.cursor()
            
            query = f"SELECT COUNT(*) FROM {self.target_config['mysql_target_table_name']}"
            cursor.execute(query)
            target_count = cursor.fetchone()[0]
            
            cursor.close()
            connection.close()
            
            logger.info(f"Validation - Source: {source_count}, Target: {target_count}")
            
            if source_count == target_count:
                logger.info("Migration validation successful!")
                return True
            else:
                logger.warning(f"Migration validation failed! Record counts don't match.")
                return False
                
        except Error as e:
            logger.error(f"Error during validation: {e}")
            return False

def main():
    """Main function with command line argument support"""
    parser = argparse.ArgumentParser(description='Migrate data between MySQL databases')
    parser.add_argument('--batch-size', type=int, default=1000, 
                       help='Number of records to process in each batch')
    parser.add_argument('--start-offset', type=int, default=0,
                       help='Starting offset for migration')
    parser.add_argument('--validate', action='store_true',
                       help='Run validation after migration')
    
    args = parser.parse_args()
    
    try:
        # Initialize migrator
        migrator = DatabaseMigrator(
            MYSQL_CONFIGS["source_db"],
            MYSQL_CONFIGS["target_db"],
            batch_size=args.batch_size
        )
        
        # Perform migration
        migrator.migrate_data(start_offset=args.start_offset)
        
        # Run validation if requested
        if args.validate:
            migrator.validate_migration()
            
    except Exception as e:
        logger.error(f"Migration process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()