CREATE TABLE snapshots.payments_2025_05_03
ENGINE = MergeTree()
ORDER BY (id)  -- Replace 'id' with actual primary key column
AS SELECT * FROM amt.payments