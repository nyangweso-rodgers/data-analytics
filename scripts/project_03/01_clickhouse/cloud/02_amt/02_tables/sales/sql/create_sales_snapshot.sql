CREATE TABLE snapshots.sales_2026_04_26
ENGINE = MergeTree()
ORDER BY (account_id)
AS SELECT * FROM amt.sales