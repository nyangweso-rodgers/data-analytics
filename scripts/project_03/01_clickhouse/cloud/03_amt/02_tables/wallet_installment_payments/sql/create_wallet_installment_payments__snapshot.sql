CREATE TABLE snapshots.wallet_installment_payments_2026_04_26
ENGINE = MergeTree()
ORDER BY (accountId, id)
AS SELECT * FROM amt.wallet_installment_payments