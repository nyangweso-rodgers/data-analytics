## Analysis: First Payment Default (FPD)

### Purpose

First Payment Default (FPD) measures whether a PAYG customer paid their first
installment (`paymentSequence = 1`) by the expected due date. It is a leading
indicator of portfolio credit risk — customers who default on their first payment
are significantly more likely to default on subsequent payments.

This analysis combines:

- `marts.mart_accounts` — account and customer details
- `amt.installment_schedules` — expected payment schedule per account
- `amt.wallet_installment_payments` — actual payments made against each schedule

### FPD Definition

An account is flagged as FPD (`is_fpd = 1`) when:

- `paymentSequence = 1` (first installment, not the deposit)
- `expectedDate` has already passed (due date is in the past)
- The account has NOT paid in full on or before the `expectedDate`

An account is NOT FPD (`is_fpd = 0`) when:

- `amountPaid >= expectedAmount` AND `paymentDate <= expectedDate`

An account is excluded from FPD (`is_fpd = NULL`) when:

- `paymentSequence <> 1` (not the first installment)
- `expectedDate > today()` (due date has not yet passed)

### Scope & Filters

- Countries: `kenya`, `uganda` only
- Account Type: `PAYG` only — CASH accounts have no installment schedules
- Included statuses: `Current`, `Arrears`, `Complete`, `Repossession`,
  `Pending Repossession`, `Write Off`, `Advance`, `Repossession On Hold`, `REPOSSESSION`
- Deleted installment schedule records are excluded via `amt.deleted_records_audit`

### Calculated Fields

- `payment_status` — Payment status per installment schedule:
  - `Unpaid` — `amountPaid = 0`
  - `Partially Paid` — `0 < amountPaid < expectedAmount`
  - `Paid in Full` — `amountPaid >= expectedAmount`

- `installment_timeline` — Timeline classification per schedule:
  - `Deposit` — `paymentSequence = 0`
  - `Past Due` — `expectedDate < today()`
  - `Due Today` — `expectedDate = today()`
  - `Upcoming` — `expectedDate > today()`

- `days_to_full_payment` — For fully paid schedules: how many days after
  `expectedDate` was the installment eventually completed. Negative = paid early,
  positive = paid late, NULL = not yet paid in full.

- `current_dpd` — Days Past Due today. For outstanding schedules where
  `expectedDate < today()`: `dateDiff('day', expectedDate, today())`.
  Returns `0` if not yet overdue.

- `is_fpd` — FPD flag. `1` = defaulted on first installment, `0` = paid on time,
  `NULL` = not applicable or due date not yet reached.

- `amountPaid` — Net amount paid after refunds:
  `wallet_installment_payments.amountPaid - wallet_installment_payments.amountRefunded`

### Three FPD Outcome Groups

| Group          | Filter                                               | Key Metric                                  |
| -------------- | ---------------------------------------------------- | ------------------------------------------- |
| Paid in Full   | `payment_status = 'Paid in Full'` AND `is_fpd = 1`   | `days_to_full_payment` — how many days late |
| Partially Paid | `payment_status = 'Partially Paid'` AND `is_fpd = 1` | `current_dpd` — how many days overdue today |
| Unpaid         | `payment_status = 'Unpaid'` AND `is_fpd = 1`         | `current_dpd` — how many days overdue today |

### Notes

- `paymentSequence = 0` is the deposit — always exclude from FPD analysis
- `paymentSequence = 1` is the first installment — the only sequence used for FPD
- One account can have multiple payment rows per schedule (multiple ledger entries)
  — the query aggregates these with `SUM(amountPaid)` and `MAX(paymentDate)`
- `amt.deleted_records_audit` is used to exclude soft-deleted installment schedule
  records that still appear in `amt.installment_schedules`
- `check_fpd_accounts_cte` at the end of the query returns only `Unpaid` FPD accounts
  — modify the final `SELECT` to include `Partially Paid` or `Paid in Full` as needed

### Canonical Base Query

```sql
WITH
-- Accounts
mart_accounts_cte AS (
    SELECT account_id, accountRef, accountType, status, customerId,
           companyRegion, product, sale_date, jsfDate
    FROM (
        SELECT account_id, accountRef, accountType, status, customerId,
               companyRegion, product, sale_date, jsfDate,
               row_number() OVER (PARTITION BY account_id ORDER BY _generated_at DESC) AS rnk
        FROM marts.mart_accounts
    ) WHERE rnk = 1
),
-- Deleted Records (to exclude soft-deleted installment schedules)
deleted_records_audit_cte AS (
    SELECT id, recordId, tableName
    FROM (
        SELECT DISTINCT id, recordId, tableName,
               row_number() OVER (PARTITION BY id ORDER BY sync_at DESC) AS rnk
        FROM amt.deleted_records_audit
    ) WHERE rnk = 1
),
-- Installment Schedules (excluding deleted records)
installment_schedules_cte AS (
    SELECT accountId, id, installmentType, paymentSequence, expectedDate, expectedAmount
    FROM (
        SELECT id, accountId, installmentType, paymentSequence, expectedAmount, expectedDate,
               row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk
        FROM amt.installment_schedules
    ) WHERE rnk = 1
    AND id NOT IN (
        SELECT recordId FROM deleted_records_audit_cte WHERE tableName = 'installment_schedules'
    )
),
-- Wallet Installment Payments
wallet_installment_payments_cte AS (
    SELECT id, accountId, instalmentScheduleId, paymentId, ledgerEntryId,
           amountPaid, amountRefunded, paymentDate
    FROM (
        SELECT DISTINCT id, accountId, instalmentScheduleId, paymentId, ledgerEntryId,
               amountPaid, amountRefunded, paymentDate,
               row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk
        FROM amt.wallet_installment_payments
    ) WHERE rnk = 1
),
-- Join accounts, schedules, and payments
installment_payments_mashup_cte AS (
    SELECT
        ma.account_id,
        ma.accountRef,
        ma.accountType,
        ma.status,
        ma.customerId,
        ma.companyRegion,
        ma.product,
        ma.sale_date,
        date(ma.jsfDate) AS jsfDate,
        isc.installmentType,
        isc.paymentSequence,
        isc.expectedDate,
        isc.expectedAmount,
        wip.paymentDate,
        (wip.amountPaid - wip.amountRefunded) AS amountPaid
    FROM mart_accounts_cte ma
    LEFT JOIN installment_schedules_cte isc ON isc.accountId = ma.account_id
    LEFT JOIN wallet_installment_payments_cte wip ON wip.instalmentScheduleId = isc.id
    WHERE ma.companyRegion IN ('kenya', 'uganda')
    AND ma.accountType = 'PAYG'
    AND ma.status IN (
        'Complete', 'Current', 'Repossession', 'Arrears',
        'Pending Repossession', 'Write Off', 'Advance',
        'Repossession On Hold', 'REPOSSESSION'
    )
),
-- Aggregate payments per schedule and calculate FPD metrics
agg_installment_payments_cte AS (
    SELECT *,
        CASE
            WHEN paymentSequence = 0 THEN 'Deposit'
            WHEN expectedDate < today() THEN 'Past Due'
            WHEN expectedDate = today() THEN 'Due Today'
            ELSE 'Upcoming'
        END AS installment_timeline,
        CASE
            WHEN amountPaid >= expectedAmount
                THEN dateDiff('day', expectedDate, paymentDate)
        END AS days_to_full_payment,
        CASE
            WHEN amountPaid < expectedAmount AND expectedDate < today()
                THEN dateDiff('day', expectedDate, today())
            ELSE 0
        END AS current_dpd,
        CASE
            WHEN ifNull(amountPaid, 0) = 0 THEN 'Unpaid'
            WHEN ifNull(amountPaid, 0) < expectedAmount THEN 'Partially Paid'
            ELSE 'Paid in Full'
        END AS payment_status,
        CASE
            WHEN paymentSequence <> 1 THEN NULL
            WHEN expectedDate > today() THEN NULL
            WHEN amountPaid >= expectedAmount AND paymentDate <= expectedDate THEN 0
            ELSE 1
        END AS is_fpd
    FROM (
        SELECT DISTINCT
            account_id, accountRef, status, customerId, companyRegion, product,
            date(sale_date) AS sale_date,
            date(jsfDate) AS jsfDate,
            installmentType, paymentSequence, expectedDate, expectedAmount,
            max(date(paymentDate)) AS paymentDate,
            coalesce(sum(amountPaid), 0) AS amountPaid
        FROM installment_payments_mashup_cte
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,13
    )
),
-- FPD accounts: Unpaid first installment past due date
check_fpd_accounts_cte AS (
    SELECT *
    FROM agg_installment_payments_cte
    WHERE paymentSequence = 1
    AND payment_status = 'Unpaid'
    AND is_fpd = 1
)
SELECT *
FROM check_fpd_accounts_cte
ORDER BY account_id, paymentSequence, expectedDate, paymentDate
```
