## Dashboard: Turn Around Time

## Purpose

To analyze the Turn Around Time (TAT) between account journey processes starting
from Sale, Sales Order Creation, Dispatch, to Installation. The dashboard shows
the datetime each event occurred and the duration in days between each step.

## Journey Steps

- Sale → Sales Order Creation
- Sales Order Creation → Dispatch
- Dispatch → Installation
- Sale → Installation (Total TAT)

## Tables

### `amt.boq`

#### Key Identifiers

- `accountId` - Unique Identifier for account. Links to `marts.mart_accounts.account_id`
- `createdAt` - Datetime when a Sales Order was created

#### Canonical Base Query

```sql
WITH boq_cte AS (
  SELECT *
  FROM (
    SELECT *,
      row_number() OVER (PARTITION BY accountId ORDER BY sync_at DESC) AS rnk
    FROM amt.boq
  ) WHERE rnk = 1
)
SELECT *
FROM boq_cte
```

### `marts.mart_accounts`

Use the canonical base query from the `mart_accounts` context as the foundation.

#### Key Identifiers

- `companyRegion` - Country name i.e., kenya, uganda, civ
- `customer_name` - Customer Name
- `customerId` - Unique Customer Identifier
- `account_id` - Unique Identifier for account. Links to `amt.boq.accountId`
- `accountRef` - Human readable account identifier
- `sale_date` - Datetime when a sale was made. NULL means a sale has not happened
- `dispatchDate` - Datetime the product was dispatched. NULL means product has not been dispatched
- `jsf_completed_date` - Datetime when a product was installed. For a confirmed installation: `engineer_recommendation = 'Installed'` AND `jsf_type = 'INSTALLATION'`
- `engineer_recommendation` - Recommendation by the field engineer. Use `'Installed'` to confirm installation
- `jsf_type` - Type of JSF visit. Use `'INSTALLATION'` to filter for installation visits only

## Join Logic

- `amt.boq.accountId` = `marts.mart_accounts.account_id`

## Event Date Columns

- `sale_date` - Date of sale (`marts.mart_accounts.sale_date`)
- `so_created_date` - Date Sales Order was created (`amt.boq.createdAt`)
- `dispatch_date` - Date product was dispatched (`marts.mart_accounts.dispatchDate`)
- `installation_date` - Date product was installed (`marts.mart_accounts.jsf_completed_date`)

## Duration Columns (in days)

- `sales_to_so_days` = `amt.boq.createdAt` - `mart_accounts.sale_date`
- `so_to_dispatch_days` = `mart_accounts.dispatchDate` - `amt.boq.createdAt`
- `dispatch_to_installation_days` = `mart_accounts.jsf_completed_date` - `mart_accounts.dispatchDate`
- `total_tat_days` = `mart_accounts.jsf_completed_date` - `mart_accounts.sale_date`

## NULL Handling

- Accounts where `sale_date` IS NULL should be excluded entirely
- If any step date is NULL, the duration for that step should show as NULL (not 0)
- Accounts where `dispatchDate` IS NULL are still in progress — include but show NULL for dispatch-related durations
- Accounts where `jsf_completed_date` IS NULL are not yet installed — include but show NULL for installation-related durations

## Dashboard Filters

- `companyRegion` - Country filter (kenya, uganda, civ)
- `account_id` / `accountRef` - Account lookup filter
- Date range filter on `sale_date`
- Dashboard should only be for `kenya` and `uganda`

## Dashboard Table Columns

- `Account Id`
- `Country`
- `Customer Id`
- `Customer Name`
- `Sale Date`
- `Sale Order Creation Date`
- `Dispatch Date`
- `Installation Date`
- `TAT (Sales -> Sale Order Creation Date)`
- `TAT (Sale Order Creation Date -> Dispatch)`
- `TAT (Dispatch -> Installation)`

## Default Sort

- Sort by `sale_date` DESC (most recent sales first)