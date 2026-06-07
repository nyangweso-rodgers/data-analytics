# Postgres

```sh
    # List available databases
    py postgres_tables_summary.py --list-databases

    # Scan all tables in a database (uses default_schema from config or "public")
    py postgres_tables_summary.py --database prod_postgres_db --tables all
    # Output: prod_postgres_db_public_report_20250514_143022.xlsx

    # Scan specific table(s)
    py postgres_tables_summary.py --database prod_postgres_db --tables accounts
    # Output: prod_postgres_db_public_accounts_report_20250514_143022.xlsx

    py postgres_tables_summary.py --database prod_postgres_db --tables accounts,users,orders
    # Output: prod_postgres_db_public_3tables_report_20250514_143022.xlsx

    # Scan with specific schema (overrides config default)
    py postgres_tables_summary.py --database prod_postgres_db --tables all --schema analytics
    # Output: prod_postgres_db_analytics_report_20250514_143022.xlsx

    # Scan all schemas in one database
    py postgres_tables_summary.py --database prod_postgres_db --tables all --schema all
    # Output: prod_postgres_db_3schemas_report_20250514_143022.xlsx

    # Scan all databases (separate Excel per database)
    py postgres_tables_summary.py --database all --tables all
    # Output: Multiple .xlsx files, one per database

    # Custom timeout for slow queries (default: 10s)
    py postgres_tables_summary.py --database prod_postgres_db --tables all --timeout 30
```
