# PostgreSQL DB Tables Summary

## Usage

```sh
    # All tables in default schema (public)
    python postgres_tables_summary.py --tables --all

    # All tables in specific schema
    python postgres_tables_summary.py --tables --all --schema analytics

    # Specific tables from public schema
    python postgres_tables_summary.py --tables users orders products

    # Specific tables from custom schema
    python postgres_tables_summary.py --tables users orders --schema staging

    # Scan with specific schema
    python postgres_tables_summary.py --all --schema public
```
