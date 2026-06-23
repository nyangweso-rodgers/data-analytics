## Database: `amt`

### Purpose

The `amt` database contains operational and product data including warranty management, used for after-sales and portfolio reporting.

## Tables

### `amt.warranty_extensions`

- One row per warranty extension per account
- Tracks product warranty periods for after-sales and portfolio reporting

#### Key Identifiers

- `accountId` - Links to `mart_accounts.account_id`
- `startDate` - Warranty start date
- `endDate` - Warranty end date / expiry date

#### Notes

#### Canonical Base Query

WITH
warranty_extensions_cte as (
select _
from (
SELECT _,
row_number()over(partition by accountId ORDER BY sync_at desc) as rnk
FROM amt.warranty_extensions
) where rnk = 1
)
select \*
from warranty_extensions_cte
