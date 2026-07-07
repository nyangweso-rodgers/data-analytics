CREATE TABLE credit_score_model.ml_payg_interventions_2026_07_07
ENGINE = MergeTree()
ORDER BY (accountId)
AS SELECT * FROM credit_score_model.ml_payg_interventions;