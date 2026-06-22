import os
import logging
from datetime import datetime
from typing import Any, Dict, List

import clickhouse_connect
import mysql.connector
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# MySQL Configuration
MYSQL_CONFIG = {
    "host": os.getenv("SC_MYSQL_AMTDB_v39_HOST"),
    "port": int(os.getenv("MYSQL_DB_PORT", "3306")),
    "username": os.getenv("SC_MYSQL_AMTDB_v39_USER"),
    "password": os.getenv("SC_MYSQL_AMTDB_v39_PASSWORD"),
    "database": os.getenv("SC_MYSQL_AMTDB_v39_DB"),
    "secure": True,
    "verify": False,
}

# ClickHouse Configuration
CLICKHOUSE_CONFIG = {
    "host": os.getenv("SC_CH_DB_HOST"),
    "port": int(os.getenv("SC_CH_DB_PORT", "8443")),
    "username": os.getenv("SC_CH_DB_USER"),
    "password": os.getenv("SC_CH_DB_PASSWORD"),
    "database": "marts",
    "secure": True,
    "verify": True,
}

CLICKHOUSE_TABLE_CONFIGS = {
    "table_name": "mart_sales",
    "batch_size": 50000,
}

MYSQL_TABLE_CONFIG = {
    "table_name": "sales_v2",
}

SYNC_PIPELINE = "dagster"

# Field mapping: clickhouse_field -> {target_name, target_type}
FIELD_MAPPING = {
    "account_id": {
        "target_name": "account_id",
        "target_type": "int",
    },
    "sale_date": {
        "target_name": "sale_date",
        "target_type": "datetime",
    },
}

# Maps the logical target_type in FIELD_MAPPING to a MySQL column type.
MYSQL_TYPE_MAP = {
    "int": "BIGINT",
    "float": "DOUBLE",
    "decimal": "DECIMAL(18,4)",
    "datetime": "DATETIME",
    "date": "DATE",
    "bool": "TINYINT(1)",
    "str": "VARCHAR(255)",
    "text": "TEXT",
}


def get_clickhouse_client():
    """Establish connection to ClickHouse"""
    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        logger.info("Successfully connected to ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise


def check_clickhouse_table_exists(client, table_name: str) -> bool:
    """Check if table exists in ClickHouse"""
    try:
        database = CLICKHOUSE_CONFIG["database"]
        query = (
            "SELECT count() FROM system.tables "
            f"WHERE database = '{database}' AND name = '{table_name}'"
        )
        result = client.query(query)
        exists = result.result_rows[0][0] > 0

        if exists:
            logger.info(f"ClickHouse table '{table_name}' exists")
        else:
            logger.error(f"ClickHouse table '{table_name}' does not exist")

        return exists
    except Exception as e:
        logger.error(f"Error checking ClickHouse table existence: {e}")
        return False


def get_mysql_client():
    """Establish connection to MySQL"""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG["host"],
            port=MYSQL_CONFIG["port"],
            user=MYSQL_CONFIG["username"],
            password=MYSQL_CONFIG["password"],
            database=MYSQL_CONFIG["database"],
            # MYSQL_CONFIG uses ClickHouse-style "secure"/"verify" keys;
            # translate them to mysql-connector's SSL options here.
            ssl_disabled=not MYSQL_CONFIG.get("secure", False),
            ssl_verify_cert=MYSQL_CONFIG.get("verify", False),
            autocommit=False,
        )
        logger.info("Successfully connected to MySQL")
        return conn
    except mysql.connector.Error as e:
        logger.error(f"Failed to connect to MySQL: {e}")
        raise


def ensure_mysql_table(conn, table_name: str) -> None:
    """Create the target MySQL table (if absent) from FIELD_MAPPING.

    Columns and types are derived from the target_name / target_type of each
    mapped field. account_id is treated as the primary key; adjust if your
    grain is different.
    """
    column_defs: List[str] = []
    for spec in FIELD_MAPPING.values():
        col = spec["target_name"]
        sql_type = MYSQL_TYPE_MAP.get(spec["target_type"], "VARCHAR(255)")
        column_defs.append(f"`{col}` {sql_type}")

    column_defs += [
        "`created_at` DATETIME",
        "`created_by` VARCHAR(255)",
        "`updated_at` DATETIME",
        "`updated_by` VARCHAR(255)",
    ]

    pk = "account_id"
    pk_clause = (
        f",\n    PRIMARY KEY (`{pk}`)"
        if pk in {s["target_name"] for s in FIELD_MAPPING.values()}
        else ""
    )

    ddl = (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n    "
        + ",\n    ".join(column_defs)
        + pk_clause
        + "\n)"
    )

    try:
        cursor = conn.cursor()
        cursor.execute(ddl)
        conn.commit()
        cursor.close()
        logger.info(f"Ensured MySQL table '{table_name}' exists")
    except mysql.connector.Error as e:
        logger.error(f"Failed to create MySQL table '{table_name}': {e}")
        raise


def sync_data_to_mysql(
    ch_client, mysql_conn, ch_table_name: str, mysql_table_name: str, batch_size: int
) -> int:
    """Stream rows from ClickHouse and batch-insert them into MySQL.

    Returns the total number of rows written. Uses INSERT ... ON DUPLICATE KEY
    UPDATE so re-runs upsert rather than duplicate (relies on the primary key
    set in ensure_mysql_table).
    """
    ch_fields = list(FIELD_MAPPING.keys())
    target_cols = [FIELD_MAPPING[f]["target_name"] for f in ch_fields]
    audit_cols = ["created_at", "created_by", "updated_at", "updated_by"]
    all_cols = target_cols + audit_cols

    select_cols = ", ".join(f"`{f}`" for f in ch_fields)
    database = CLICKHOUSE_CONFIG["database"]
    select_sql = f"SELECT {select_cols} FROM `{database}`.`{ch_table_name}`"

    col_list = ", ".join(f"`{c}`" for c in all_cols)
    placeholders = ", ".join(["%s"] * len(all_cols))
    # created_at / created_by are set on first insert only; updated_* refresh every run.
    update_cols = target_cols + ["updated_at", "updated_by"]
    update_clause = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols)
    insert_sql = (
        f"INSERT INTO `{mysql_table_name}` ({col_list}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {update_clause}"
    )

    sync_time = datetime.now()
    audit_values = (sync_time, SYNC_PIPELINE, sync_time, SYNC_PIPELINE)

    cursor = mysql_conn.cursor()
    total = 0
    batch: List[Any] = []

    try:
        # clickhouse_connect returns native Python types (int, datetime, ...),
        # so values can be passed straight to mysql-connector.
        with ch_client.query_rows_stream(select_sql) as stream:
            for row in stream:
                batch.append(tuple(row) + audit_values)
                if len(batch) >= batch_size:
                    cursor.executemany(insert_sql, batch)
                    mysql_conn.commit()
                    total += len(batch)
                    logger.info(f"Wrote {total} rows so far...")
                    batch = []

        if batch:
            cursor.executemany(insert_sql, batch)
            mysql_conn.commit()
            total += len(batch)

        logger.info(f"Finished writing {total} rows to '{mysql_table_name}'")
        return total
    except Exception as e:
        mysql_conn.rollback()
        logger.error(f"Error during sync: {e}")
        raise
    finally:
        cursor.close()


def main():
    ch_client = None
    mysql_conn = None
    ch_table_name = CLICKHOUSE_TABLE_CONFIGS["table_name"]
    mysql_table_name = MYSQL_TABLE_CONFIG["table_name"]
    batch_size = CLICKHOUSE_TABLE_CONFIGS["batch_size"]

    try:
        ch_client = get_clickhouse_client()

        if not check_clickhouse_table_exists(ch_client, ch_table_name):
            logger.error("Aborting: source table not found in ClickHouse")
            return

        mysql_conn = get_mysql_client()
        ensure_mysql_table(mysql_conn, mysql_table_name)

        total = sync_data_to_mysql(ch_client, mysql_conn, ch_table_name, mysql_table_name, batch_size)
        logger.info(f"Sync complete: {total} rows synced to MySQL table '{mysql_table_name}'")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise
    finally:
        if mysql_conn is not None:
            mysql_conn.close()
        if ch_client is not None:
            ch_client.close()


if __name__ == "__main__":
    main()
