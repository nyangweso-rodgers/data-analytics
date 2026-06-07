CREATE TABLE snapshots.installment_payments_2026_04_28
ENGINE = MergeTree()
ORDER BY (id)
AS SELECT * FROM amt.installment_payments