## Database: `amt`

### Purpose

The `amt` database contains operational and product data including warranty management, used for after-sales and portfolio reporting.

## Tables

### `amt.installment_schedules`

- One row per installment schedule per account. Tracks the expected payment schedule for PAYG accounts.
- Join with `amt.wallet_installment_payments` on `amt.installment_schedules.id` = `amt.wallet_installment_payments.instalmentScheduleId` to track actual payments against expected.

#### Key Identifiers

- `id` - Unique Installment Schedule identifier. Links to `amt.wallet_installment_payments.instalmentScheduleId`
- `accountId` - Unique account identifier.
- `paymentSequence` - Installment schedule sequence starting from `0`, where:
  - `0` = Deposit
  - `1` onwards = Installment schedules in order
- `expectedAmount` - Expected payment amount for each schedule
- `expectedDate` - Expected due date for each schedule

#### Notes

- Only applicable to `PAYG` accounts — `CASH` accounts have no installment schedules
- One account can have many installment schedule rows — one per payment sequence
- Use `paymentSequence = 0` to isolate deposit records
- Compare `expectedAmount` vs `amt.wallet_installment_payments.amountPaid` to determine underpayment or overpayment per schedule

#### Canonical Base Query

```sql
WITH installment_schedules_cte AS (
    SELECT *
    FROM (
        SELECT *,
        row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk
        FROM amt.installment_schedules
    ) WHERE rnk = 1
)
SELECT *
FROM installment_schedules_cte
```

---

### `amt.ptps`

One row per Promise to Pay (PTP) request per account. Tracks accounts that have
made a request or promise to pay their overdue arrears by a specific date. A
single account can have multiple PTP requests over time.

#### Key Identifiers

- `id` - Unique PTP identifier (primary dedup key)
- `accountId` - Unique account identifier. Links to `marts.mart_accounts.account_id`

#### PTP Details

- `reason` - Reason provided by the customer for requesting the PTP
- `comments` - Additional comments or notes logged against the PTP request
- `createdAt` - Date the PTP request was logged in the system
- `ptpDate` - Promise date — the date the customer has committed to m

#### Notes

- A single account can have multiple PTP records — one per request over time
- To get the most recent PTP per account, filter on `MAX(createdAt) GROUP BY accountId`
- `comments` and `reason` are free-text fields — values may be inconsistent across records

```sql
  WITH
--------------------- PTPs ----------------------------------
ptps_cte as (
    select *
    from (
        SELECT *,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk
    FROM amt.ptps
    ) where rnk = 1
    ORDER BY accountId, createdAt desc
    )
select *
from ptps_cte
```

---

### `amt.wallet_installment_payments`

- One row per ledger entry per installment schedule per account.
- Tracks actual payment allocations against each installment schedule.
- A single installment schedule can be settled by one or more ledger entries.

#### Key Identifiers

- `accountId` - Unique account identifier.
- `instalmentScheduleId` - Links to `amt.installment_schedules.id`
- `ledgerEntryId` - Unique identifier for each ledger entry (payment transaction)
- `amountPaid` - Actual amount paid to settle the schedule
- `paymentDate` - Date the payment was made

#### Notes

- One `instalmentScheduleId` can have multiple rows — one per `ledgerEntryId`
- To get total amount paid per schedule, aggregate:
  `SUM(amountPaid) GROUP BY instalmentScheduleId`
- `refundDate IS NOT NULL` indicates a refunded payment — always exclude these
  from payment analysis (already applied in canonical base query)
- Compare `amountPaid` vs `amt.installment_schedules.expectedAmount` to
  determine settlement status per schedule

#### Canonical Base Query

```sql
WITH wallet_installment_payments_cte AS (
    SELECT *
    FROM (
        SELECT *,
        row_number() OVER (PARTITION BY ledgerEntryId ORDER BY updatedAt DESC) AS rnk
        FROM amt.wallet_installment_payments
        WHERE refundDate IS NULL
    ) WHERE rnk = 1
)
SELECT *
FROM wallet_installment_payments_cte
ORDER BY paymentDate
```

### `amt.warranty_extensions`

- One row per warranty extension per account
- Tracks product warranty periods for after-sales and portfolio reporting

#### Key Identifiers

- `accountId` - Links to `mart_accounts.account_id`
- `startDate` - Warranty start date
- `endDate` - Warranty end date / expiry date

#### Canonical Base Query

```sql
WITH
warranty_extensions_cte as (
  select *
  from (
    SELECT *,
      row_number() over (partition by accountId ORDER BY sync_at desc) as rnk
    FROM amt.warranty_extensions
  ) where rnk = 1
)
select *
from warranty_extensions_cte
```
