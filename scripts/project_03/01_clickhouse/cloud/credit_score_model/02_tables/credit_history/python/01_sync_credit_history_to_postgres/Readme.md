# Sync Credit History to PostgreSQL

```sh
   # Truncate destination table before sync
   py sync_credit_history_to_postgres.py --truncate-destination-table true

   # With debug logging
   py sync_credit_history_to_postgres.py --debug

   # Both truncate and debug
   py sync_credit_history_to_postgres.py --truncate-destination-table true --debug
```
