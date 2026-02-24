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

def main():
    """Main execution entry point."""
    try:
        client = get_clickhouse_client()
        result = client.execute("SELECT 1")
        print("ClickHouse connection test result:", result)
    except Exception:
        logger.exception("Connection test failed")


if __name__ == "__main__":
    main()