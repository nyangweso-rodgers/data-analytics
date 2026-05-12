CREATE TABLE snapshots.agg_credit_history_v1_2026_05_12
ENGINE = MergeTree()
ORDER BY (accountId)
AS SELECT * FROM credit_score_model.agg_credit_history_v1;