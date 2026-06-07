CREATE TABLE snapshots.accounts_2026_05_08
ENGINE = MergeTree()
ORDER BY (id)
AS SELECT * FROM amt.accounts;