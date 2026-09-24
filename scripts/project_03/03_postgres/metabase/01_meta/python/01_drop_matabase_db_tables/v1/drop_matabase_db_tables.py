import os
from dotenv import load_dotenv
import logging
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from typing import Dict, List, Tuple
import sys
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

DB_CONFIGS = {
    "sc_postgres_metabase_db": {
        "type": "postgres",
        "host": os.getenv("SC_POSTGRES_METABASE_DB_HOST"),
        "database": os.getenv("SC_POSTGRES_METABASE_DB_NAME"),
        "username": os.getenv("SC_POSTGRES_METABASE_DB_USER"),
        "password": os.getenv("SC_POSTGRES_METABASE_DB_PASSWORD"),
        "schema": os.getenv("SC_POSTGRES_METABASE_DB_SCHEMA", "public"),
        "port": int(os.getenv("SC_POSTGRES_METABASE_DB_PORT", "5432")),
    },
}

# Leave a DB's list empty to mean "every table in the schema" (discovered live).
DB_TABLES = {
    "sc_postgres_metabase_db": [],
}

# SAFETY: Define protected databases (add production DBs here)
PROTECTED_DATABASES = [
    # Add more production database names here
]


def establish_connection_to_postgres(config: Dict) -> psycopg2.extensions.connection:
    """Establish connection to PostgreSQL database"""
    try:
        connection = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            user=config["username"],
            password=config["password"],
            dbname=config["database"],
            connect_timeout=10,  # Connection timeout (seconds)
            # statement_timeout is in milliseconds; queries longer than 30s are killed
            options="-c statement_timeout=30000",
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        # We control commits explicitly, so disable autocommit
        connection.autocommit = False
        logger.info(f"✓ Connected to PostgreSQL: {config['database']}")
        return connection
    except Exception as e:
        logger.error(f"✗ Failed to connect to PostgreSQL {config['database']}: {e}")
        return None


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="⚠️  Dangerous PostgreSQL Table Dropper - Use with caution!",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Drop all tables (interactive mode). If the DB has no configured list,
  # every table in the schema is discovered from the database and dropped.
  python drop_metabase_db_tables.py
  python drop_metabase_db_tables.py --tables all

  # Drop specific table(s)
  python drop_metabase_db_tables.py --tables customers
  python drop_metabase_db_tables.py --tables customers accounts

  # Specify database
  python drop_metabase_db_tables.py --db sc_postgres_metabase_db --tables all

  # Use exact row counts (slower)
  python drop_metabase_db_tables.py --tables all --exact-count

  # Disable CASCADE (will fail if other objects depend on the tables)
  python drop_metabase_db_tables.py --tables all --no-cascade
        """,
    )

    parser.add_argument(
        "--tables",
        "-t",
        nargs="+",
        help='Table name(s) to drop, or "all" for all available tables',
    )

    parser.add_argument(
        "--db",
        "-d",
        choices=list(DB_CONFIGS.keys()),
        default=list(DB_CONFIGS.keys())[0] if DB_CONFIGS else None,
        help="Database configuration to use (default: first configured DB)",
    )

    parser.add_argument(
        "--exact-count",
        action="store_true",
        help="Use exact row counts instead of estimates (slower)",
    )

    parser.add_argument(
        "--no-cascade",
        action="store_true",
        help="Drop without CASCADE (fails if dependent objects exist)",
    )

    return parser.parse_args()


def fetch_all_tables(
    connection: psycopg2.extensions.connection, schema: str
) -> List[str]:
    """Return every ordinary (base) table in the given schema."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s
                  AND c.relkind = 'r'
                ORDER BY c.relname
                """,
                (schema,),
            )
            return [row["table_name"] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"✗ Failed to list tables in schema '{schema}': {e}")
        connection.rollback()
        return []


def resolve_available_tables(
    connection: psycopg2.extensions.connection, db_key: str, schema: str
) -> Tuple[List[str], str]:
    """The set of tables we're allowed to act on.

    Uses the configured DB_TABLES list if it has entries; otherwise falls back
    to discovering every table in the schema from the live database.
    """
    configured = DB_TABLES.get(db_key, [])
    if configured:
        return list(configured), "configuration"

    discovered = fetch_all_tables(connection, schema)
    return discovered, "live database"


def get_tables_to_drop(args, available_tables: List[str]) -> List[str]:
    """Determine which tables to drop based on arguments"""
    if not args.tables:
        # No --tables argument -> all available tables
        return list(available_tables)

    if len(args.tables) == 1 and args.tables[0].lower() == "all":
        # --tables all -> all available tables
        return list(available_tables)

    # Specific table(s) requested -> validate against what's available
    requested_tables = args.tables

    invalid_tables = [t for t in requested_tables if t not in available_tables]
    if invalid_tables:
        logger.warning(f"⚠️  Tables not found: {', '.join(invalid_tables)}")
        logger.info(f"Available tables: {', '.join(available_tables)}")
        response = input("Continue with valid tables only? (y/n): ")
        if response.lower() != "y":
            return []

    return [t for t in requested_tables if t in available_tables]


def check_safety(config: Dict, tables: List[str]) -> bool:
    """Safety checks before dropping tables"""
    database_name = config["database"]

    # Check if database is in protected list
    if database_name in PROTECTED_DATABASES:
        logger.error(f"🛑 BLOCKED: '{database_name}' is a protected database!")
        return False

    # Check if it's a test/dev database
    if not any(
        keyword in database_name.lower()
        for keyword in ["test", "dev", "local", "staging"]
    ):
        logger.warning(
            f"⚠️  WARNING: '{database_name}' doesn't appear to be a test database!"
        )
        response = input(
            f"Are you ABSOLUTELY sure you want to drop tables from '{database_name}'? (type 'YES' to confirm): "
        )
        if response != "YES":
            logger.info("Operation cancelled by user")
            return False

    return True


def get_table_info(
    connection: psycopg2.extensions.connection, schema: str, table: str
) -> Dict:
    """Get exact table row count and size"""
    try:
        with connection.cursor() as cursor:
            # Get exact row count
            cursor.execute(
                sql.SQL("SELECT COUNT(*) AS count FROM {}").format(
                    sql.Identifier(schema, table)
                )
            )
            count = cursor.fetchone()["count"]

            # Get table size (table + indexes + toast)
            cursor.execute(
                "SELECT ROUND(pg_total_relation_size(%s) / 1024.0 / 1024.0, 2) AS size_mb",
                (f"{schema}.{table}",),
            )
            size_result = cursor.fetchone()
            size_mb = size_result["size_mb"] if size_result else 0

            return {"rows": count, "size_mb": size_mb}
    except Exception as e:
        logger.warning(f"Could not get info for {table}: {e}")
        # A failed query aborts the transaction in Postgres; roll back to recover
        connection.rollback()
        return {"rows": "?", "size_mb": "?"}


def confirm_table_deletion(
    connection: psycopg2.extensions.connection,
    tables: List[str],
    db_name: str,
    schema: str,
    fast_mode: bool = True,
) -> bool:
    """Show table info and get user confirmation

    Args:
        fast_mode: If True, uses estimated row counts (instant, from pg_class.reltuples)
                   instead of exact COUNT(*) counts (slow).
    """
    print("\n" + "─" * 60)
    print(f"📊 Tables to be DELETED from '{db_name}.{schema}':")
    print("─" * 60)

    if fast_mode:
        # Use pg_class for instant estimates (no table scan).
        # reltuples is maintained by ANALYZE/VACUUM and may be stale or -1
        # for tables that have never been analyzed.
        results = {}
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        c.relname AS table_name,
                        c.reltuples::bigint AS table_rows,
                        ROUND(pg_total_relation_size(c.oid) / 1024.0 / 1024.0, 2) AS size_mb
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s
                      AND c.relkind = 'r'
                      AND c.relname = ANY(%s)
                    """,
                    (schema, tables),
                )
                results = {row["table_name"]: row for row in cursor.fetchall()}
        except Exception as e:
            logger.warning(f"Could not fetch table stats: {e}")
            connection.rollback()
            results = {}

        total_rows = 0
        for i, table in enumerate(tables, 1):
            if table in results:
                rows = results[table]["table_rows"]
                size = results[table]["size_mb"]
                # reltuples can be -1 (unknown) for never-analyzed tables
                rows_display = rows if rows is not None and rows >= 0 else "?"
                print(
                    f"  {i}. {table:30s} (~{str(rows_display):>9} rows, {size:>6} MB)"
                )
                total_rows += rows if isinstance(rows, int) and rows > 0 else 0
            else:
                print(f"  {i}. {table:30s} (unknown)")

        print("─" * 60)
        print(f"  TOTAL: {len(tables)} tables, ~{total_rows:,} rows (estimated)")
    else:
        # Exact counts - slower but accurate
        total_rows = 0
        for i, table in enumerate(tables, 1):
            info = get_table_info(connection, schema, table)
            rows = info["rows"]
            size = info["size_mb"]
            print(f"  {i}. {table:30s} ({str(rows):>10} rows, {str(size):>6} MB)")
            if isinstance(rows, int):
                total_rows += rows

        print("─" * 60)
        print(f"  TOTAL: {len(tables)} tables, ~{total_rows:,} rows (exact)")

    print("─" * 60)

    response = input("\n⚠️  Type 'DELETE' to confirm deletion: ")
    return response == "DELETE"


def drop_postgres_tables(
    connection: psycopg2.extensions.connection,
    tables: List[str],
    schema: str,
    cascade: bool = True,
):
    """Drop tables in PostgreSQL database.

    PostgreSQL has no equivalent of MySQL's `SET FOREIGN_KEY_CHECKS = 0`.
    To drop tables that reference each other (or are referenced by other
    objects), CASCADE is used so dependent foreign keys / views are removed.
    Use cascade=False to require a clean drop with no dependents.
    """
    try:
        with connection.cursor() as cursor:
            for table in tables:
                if cascade:
                    drop_query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(schema, table)
                    )
                else:
                    drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
                        sql.Identifier(schema, table)
                    )
                cursor.execute(drop_query)
                logger.info(f"  ✓ Dropped table: {schema}.{table}")

        connection.commit()
        logger.info("✓ All tables dropped successfully")
    except Exception as e:
        logger.error(f"✗ Failed to drop tables: {e}")
        connection.rollback()
        raise


def main():
    """Main execution function"""
    args = parse_arguments()

    print("\n" + "=" * 60)
    print("⚠️  DANGEROUS DATABASE CLEANUP SCRIPT ⚠️")
    print("=" * 60 + "\n")

    # Determine which database to use
    db_key = args.db
    if not db_key:
        logger.error("No database configuration available")
        sys.exit(1)

    config = DB_CONFIGS[db_key]
    schema = config.get("schema", "public")
    cascade = not args.no_cascade

    # Safety check (based on DB name) BEFORE we even connect
    if not check_safety(config, []):
        logger.warning(f"Skipping {db_key} due to safety check")
        sys.exit(1)

    # Establish connection (needed to discover tables when none are configured)
    print(f"\n🔌 Connecting to {db_key}...")
    connection = establish_connection_to_postgres(config)
    if not connection:
        sys.exit(1)

    print(
        f"✅ Connection to PostgreSQL '{config['database']}' established successfully!\n"
    )

    try:
        # Figure out the available tables (config list, or every table in schema)
        available_tables, source = resolve_available_tables(connection, db_key, schema)
        if not available_tables:
            logger.error(
                f"No tables found in schema '{schema}' (source: {source}). Nothing to do."
            )
            sys.exit(1)

        # Determine which of the available tables to drop
        tables_to_drop = get_tables_to_drop(args, available_tables)
        if not tables_to_drop:
            logger.error("No valid tables specified")
            sys.exit(1)

        print(f"🔌 Target Database: {config['database']} (schema: {schema})")
        print(f"📋 Table source: {source}")
        print(f"📋 Tables to drop: {', '.join(tables_to_drop)}")
        print(
            f"🔢 Count mode: {'exact (slow)' if args.exact_count else 'estimated (fast)'}"
        )
        print(f"🔗 CASCADE: {'enabled' if cascade else 'disabled'}")
        print()

        # Show table info and get confirmation
        fast_mode = not args.exact_count
        if not confirm_table_deletion(
            connection,
            tables_to_drop,
            config["database"],
            schema,
            fast_mode=fast_mode,
        ):
            logger.info("❌ Operation cancelled by user")
            sys.exit(0)

        # Drop tables
        print("\n🗑️  Dropping tables...\n")
        drop_postgres_tables(connection, tables_to_drop, schema, cascade=cascade)
        print("\n✅ All operations completed successfully!")

    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"Error during table drop: {e}")
        sys.exit(1)
    finally:
        connection.close()
        logger.info(f"🔌 Connection closed for {db_key}")

    print("\n" + "=" * 60)
    print("Script execution completed")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user (Ctrl+C)")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
