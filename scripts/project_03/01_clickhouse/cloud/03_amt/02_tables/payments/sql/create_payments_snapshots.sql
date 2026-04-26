CREATE TABLE snapshots.payments_2025_03_08
ENGINE = MergeTree()
ORDER BY (id)  -- Replace 'id' with your actual primary key column
AS SELECT * FROM snapshot.payments_2025_03_08