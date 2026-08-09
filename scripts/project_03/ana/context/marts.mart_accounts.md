## Table: `marts.mart_accounts`

### Purpose

One row per account. Central reference table for account lifecycle analysis including sales, dispatches, installations, refunds, and portfolio reporting.

### Key Identifiers

- `companyRegion` - Country name i.e., kenya, uganda, and civ
- `region` - Customer region within a country
- `account_id` — Primary key. Unique per account.
- `accountRef` - similar to `account_id` another unique identifier per account. Human readable.
- `customerId` — Unique Customer Id. Each customer has several accounts, i.e., `account_id` or `accountRef`
- `customer_name` - Name of the Customer
- `salesAgents` - Unique Identifier of the Sales Agent who sold to the account
- `supervisor_id` - Unique Identifier of the Sales Agent Supervisor
- `supervisor_name` - Supervisor name

### Customer Classification

- `gender` - Customer Gender
- `customerType` - Distributor, Individual, Partner. Determines the type of customer
- `latitude` - customer latitude
- `longitude` - customer longitude
- `County` - customer County - specifically for Kenya, since Uganda does not have County concept
- `subcounty` - customer subcounty
- `town` - customer town

### Account Classification

- `accountType` — `PAYG`, `CASH`, or `ADDON`. Determines payment structure.
- For all PAYG Accounts set `accountType = PAYG`
- `status` — Current lifecycle state: `Current`, `Arrears`, `Complete`, etc.

### Product Classification

- `category` - Product category including `Pump`, `Add-On (TV, Direct Drip (DD))`, `Other ('Kilimo Boost')`
- `productId` - Unique Product Identifier
- `productName` — Name of the product sold on the account
- Pump Sales = all products EXCLUDING: 'Kilimo Boost', 'TV', 'Direct Drip (DD)'
- Refurb products are flagged via `isRefurb = 1`

### Dates

- `createdAt` - Date the account was created in the database
- `sale_date` — Date the a sale was done for the account
- `dispatchDate` — Date the product was dispatched.
- `jsf_completed_date` — Date the product was physically installed.

### Notes on Sales

- `sale_date` must not be null
- Exclude status: `No Deposit`, `No Deposit `, `Refunded`

### Notes on Installations

- For installations:
- `jsf_date` is the installation date.
- `jsf_date` is not NULL,and `jsf_type = 'INSTALLATION'`, and `engineer_recommendation = 'Installed'`

### Notes on Accounts

- `arrears` - total arrears for each account: what is currently owed, including previous obligations plus what is expected as of now.

### Account Type Notes

- `PAYG` accounts follow an installment payment plan — use installment tables for payment analysis
- `CASH` accounts are fully paid upfront — no installment schedule applies

### Refunded Accounts Notes

- For Full Refunds: `sale_date` is and `RefundDate` is NOT NULL

### Canonical Base Query

Note: the raw table holds one row per account _snapshot_ (a new row is written each time an account changes), so the query below dedupes to the latest snapshot per account before use.

```sql
WITH
mart_accounts_cte as (
  select *
  from (
    SELECT *,
      row_number() over (partition by account_id ORDER BY _generated_at desc) as rnk
    FROM marts.mart_accounts
  ) where rnk = 1
  and productName not in ('Transport / Shipping', 'TSR', 'TSR Uganda', 'Training', 'UG Extra Items', 'Extra Items', 'Installation', 'furrow', 'AfterSale', 'Agronomy', 'Samsung Galaxy A11')
  and customer_name NOT LIKE '%Test%'
)
select *
from mart_accounts_cte
```
