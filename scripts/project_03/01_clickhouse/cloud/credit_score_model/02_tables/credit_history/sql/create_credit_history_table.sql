CREATE TABLE IF NOT EXISTS credit_score_model.credit_history_v1
(
    country String,
    customer_id Int32,
    account_id Int32,
    account_ref Nullable(String),
    account_status String,
    product_name Nullable(String),
    deposit_amount Nullable(Float64),
    installment_amount Nullable(Float64),
    payment_sequence Nullable(Int32),
    expected_date Nullable(Date),
    expected_amount Nullable(Float64),
    amount_paid Float64,
    total_number_payments Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (customer_id, account_id);