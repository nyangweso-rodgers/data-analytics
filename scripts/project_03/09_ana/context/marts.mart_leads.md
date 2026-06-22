## Table: marts.mart_leads

### Purpose

One row per lead. Central reference table for leads lifecycle analysis including lead generation, lead source for reporting.

### Key Identifiers

- `country` - Country name i.e., kenya, uganda, and civ
- `leadId` - Unique Lead Identifier

### Customer Information

- `firstName` - First Name of the Customer
- `middleName` - Middle Name of the customer
- `lastName` - Last Name of the Customer
- `name` - Full Names of the Customer
- `idNumber` - Unique Identification Number of Customer

### Canonical Base Query

WITH
mart_leads_cte as (
select _
from (
SELECT _,
row_number()over(partition by leadId ORDER BY \_generated_at desc) as rnk
FROM marts.mart_leads
) where rnk = 1
)
select \*
from mart_leads_cte
