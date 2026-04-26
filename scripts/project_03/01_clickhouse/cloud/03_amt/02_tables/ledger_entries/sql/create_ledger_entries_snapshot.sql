CREATE TABLE snapshots.ledger_entries_2026_04_26
ENGINE = MergeTree()
ORDER BY (account_id, id)
AS SELECT * FROM amt.ledger_entries