# Usage

# Basic sync with default batch size (1000)

python sync_cds_to_postgres.py

# Custom batch size

python sync_cds_to_postgres.py --batch-size 5000

# Verify only (no sync)

python sync_cds_to_postgres.py --verify-only
