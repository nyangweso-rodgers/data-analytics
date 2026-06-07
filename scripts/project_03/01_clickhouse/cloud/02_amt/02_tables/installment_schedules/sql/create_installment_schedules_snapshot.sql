CREATE TABLE snapshots.installment_schedules_2026_05_08
ENGINE = MergeTree()
ORDER BY (id)
AS SELECT * FROM amt.installment_schedules;