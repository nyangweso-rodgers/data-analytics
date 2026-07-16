CREATE TABLE snapshots.accounts_2026_05_15
ENGINE = MergeTree()
ORDER BY (id)
AS SELECT * FROM amt.accounts;