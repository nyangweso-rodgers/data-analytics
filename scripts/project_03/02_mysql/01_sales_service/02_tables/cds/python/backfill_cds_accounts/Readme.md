# 1. Dry run first to see what would happen

py backfill_cds_accounts.py --customer-ids 54201

# 2. Check the output carefully - verify the field values shown

# 3. When ready, run live

py backfill_cds_accounts.py --live --customer-ids 54201

# 4. Validate

py validate_backfill.py --customer-ids 54201
