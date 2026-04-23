CREATE VIEW marts.vw_payments  AS
SELECT
    account_id,
    amount,
    payment_ref,
    timestamp_made,
    payment_id
FROM
    (
        SELECT
            toInt32(rc.amt_account_id) AS account_id,
            multiIf(
                p.status = 'REVERSED',
                0,
                (p.debit > 0)
                AND (p.credit = 0),
                0,
                p.credit - p.debit
            ) AS amount,
            p.reference AS payment_ref,
            p.transaction_timestamp AS timestamp_made,
            p.id AS payment_id,
            row_number() OVER (
                PARTITION BY p.id
                ORDER BY
                    p.transaction_timestamp DESC
            ) AS rn
        FROM
            amt.ledger_entries AS p
            INNER JOIN amt.customer_payment_accounts AS rc ON toInt32(p.account_id) = rc.id
            INNER JOIN amt.accounts AS a ON a.id = toInt32(rc.amt_account_id)
        WHERE
            (toInt32(rc.amt_account_id) IS NOT NULL)
            AND (a.isMigrated = 1)
    )
WHERE
    rn = 1