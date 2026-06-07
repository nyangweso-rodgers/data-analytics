# MySQL Database Table Summary

## Table of Contents

```sh
    # Scan all tables in a specific database
    py mysql_tables_summary.py --database sc_mysql_amtdb --tables all
    # Output: sc_mysql_amtdb_report_20250514_143022.xlsx

    # Scan specific table(s) in a database
    py mysql_tables_summary.py --database sc_mysql_amtdb --tables accounts
    # Output: sc_mysql_amtdb_accounts_report_20250514_143022.xlsx

    py mysql_tables_summary.py --database sc_mysql_amtdb --tables accounts,users,orders
    # Output: sc_mysql_amtdb_3tables_report_20250514_143022.xlsx

    # Scan all tables across ALL databases (separate Excel per database!)
    py mysql_tables_summary.py --database all --tables all
    # Output:
    #   sc_mysql_amtdb_report_20250514_143022.xlsx
    #   sc_mysql_sales_service_report_20250514_143022.xlsx
    #   local_mysql_kaleidofin_db_report_20250514_143022.xlsx
    #   ...

    # List available databases
    py mysql_tables_summary.py --list-databases
```
