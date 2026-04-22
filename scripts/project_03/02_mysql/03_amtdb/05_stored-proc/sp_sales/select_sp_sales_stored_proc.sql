WITH
  base_payments AS (
    SELECT
      vp.account_id,
      vp.payment_id,
      vp.timestamp_made,
      SUM(vp.amount) AS amount
    FROM
      vw_payments2 vp
    WHERE
      vp.payment_ref <> 'Refund'
    GROUP BY
      vp.account_id,
      vp.payment_id,
      vp.timestamp_made
  ),
  refunds_per_payment AS (
    SELECT
      r.paymentId,
      SUM(r.refundAmount) AS refundAmount
    FROM
      refunds r
    WHERE
      r.paymentId <> 1
      AND r.paymentId IS NOT NULL
      AND r.status = 'APPROVED'
    GROUP BY
      r.paymentId
  ),
  payments_with_refunds AS (
    SELECT
      bp.account_id,
      bp.payment_id,
      bp.timestamp_made,
      bp.amount,
      COALESCE(r.refundAmount, 0) AS refundAmount,
      (bp.amount - COALESCE(r.refundAmount, 0)) AS net_amount
    FROM
      base_payments bp
      LEFT JOIN refunds_per_payment r ON bp.payment_id = r.paymentId
  ),
  payments_with_cumulative AS (
    SELECT
      p.account_id,
      p.payment_id,
      p.timestamp_made,
      p.amount,
      p.refundAmount,
      p.net_amount,
      SUM(p.net_amount) OVER (
        PARTITION BY
          p.account_id
        ORDER BY
          p.timestamp_made ROWS BETWEEN UNBOUNDED PRECEDING
          AND CURRENT ROW
      ) AS cumulative
    FROM
      payments_with_refunds p
  ),
  discounts AS (
    SELECT
      dn.accountId,
      SUM(dn.amount) AS discount_amount
    FROM
      discount_notes dn
      LEFT JOIN discount_codes dc ON dn.discountId = dc.id
    WHERE
      dc.discountName NOT LIKE '%discount on installment%'
    GROUP BY
      dn.accountId
  ),
  account_refunds AS (
    SELECT
      r.accountId,
      SUM(r.refundAmount) AS refundAmount
    FROM
      refunds r
    WHERE
      (
        r.paymentId = 1
        OR (
          r.paymentId IS NULL
          AND r.status = 'APPROVED'
        )
      )
    GROUP BY
      r.accountId
  ),
  deposits AS (
    SELECT
      a.id AS account_id,
      pp.depositAmount
    FROM
      accounts a
      LEFT JOIN account_payplans ap ON a.id = ap.accountId
      LEFT JOIN payplans pp ON ap.payplanId = pp.id
    GROUP BY
      a.id,
      pp.depositAmount
  ),
  final_existing AS (
    SELECT
      p.account_id,
      p.timestamp_made,
      (
        COALESCE(p.cumulative, 0) + COALESCE(d.discount_amount, 0) - COALESCE(rf.refundAmount, 0)
      ) AS cumulative_with_discount,
      d.discount_amount,
      p.refundAmount,
      dep.depositAmount,
      CASE
        WHEN (
          COALESCE(p.cumulative, 0) + COALESCE(d.discount_amount, 0) - COALESCE(rf.refundAmount, 0)
        ) >= dep.depositAmount THEN 1
        ELSE 0
      END AS deposit_paid
    FROM
      payments_with_cumulative p
      LEFT JOIN discounts d ON p.account_id = d.accountId
      LEFT JOIN account_refunds rf ON p.account_id = rf.accountId
      LEFT JOIN deposits dep ON p.account_id = dep.account_id
  ),
  final as (
    select
      *
    from
      final_existing
    where
      deposit_paid = 1
  ),
  new_customers AS (
    SELECT
      a.id AS account_id,
      MIN(p.timestampMade) AS timestamp_made,
      -- cumulative payments
      COALESCE(SUM(p.amount), 0)
        + COALESCE(d.discount_amount, 0)
        - COALESCE(rf.refundAmount, 0) AS cumulative_with_discount,
      d.discount_amount,
      rf.refundAmount,
      dep.depositAmount,
      CASE 
        WHEN (
          COALESCE(SUM(p.amount), 0)
          + COALESCE(d.discount_amount, 0)
          - COALESCE(rf.refundAmount, 0)
        ) >= dep.depositAmount
        THEN 1 
        ELSE 0 
      END AS deposit_paid
    FROM customer_payment_accounts cpa
      INNER JOIN payments p ON p.customerId = cpa.customer_id
      INNER JOIN accounts a ON a.customerId = cpa.customer_id
      LEFT JOIN discounts d ON a.id = d.accountId
      LEFT JOIN account_refunds rf ON a.id = rf.accountId
      LEFT JOIN deposits dep ON a.id = dep.account_id
      LEFT JOIN final f ON a.id = f.account_id
    WHERE f.account_id IS NULL  -- exclude existing deposit met
    GROUP BY a.id
  ),
  combined AS (
    SELECT
      *
    FROM
      final
    UNION ALL
    SELECT
      *
    FROM
      new_customers
  )/*
SELECT
  c.account_id,
  MIN(c.timestamp_made) AS sale_date,
  MIN(c.timestamp_made) AS updatedAt
FROM
  combined c
WHERE
  c.deposit_paid = 1
GROUP BY
  c.account_id
ORDER BY
  sale_date DESC ON DUPLICATE KEY
UPDATE
  sale_date =
VALUES
  (sale_date);

END*/
select *
from base_payments
#from combined 
where account_id = '70821'