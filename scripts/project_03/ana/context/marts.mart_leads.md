## Table: marts.mart_leads

### Purpose

One row per lead. Central reference table for leads lifecycle analysis including lead generation, lead source for reporting.

### Key Identifiers

- `country` - Country name i.e., kenya, uganda, and civ
- `leadId` - Unique Lead Identifier
- `leadsource` - Leadsource including `Reshuffled Leads`, `Door to Door`, `Facebook`, e.t.c.
- `leadchannel` - Lead Channel
- `is_migrated` - Flag showing whther the Lead record was migrated from the Legacy System (i.e., Salesforce) into the new/current Platform (Tunda). `1` is for migrated lead, `0` for non-migrated lead.
- `preferredLanguage` - Metadata for Customer Prefered Language
- `createdById` - Unique Identifier of the agent who originally registered/created the lead in the system.
- `createdByName` - Agent name who originally registered/created the lead in the system.
- `agentId` - Unique Identifier of the person who currently the lead has been asisgned to. There are instances where once the lead is created, it is later assigned to a different person.
- `agentName` - Name of person who currently the lead has been assigned to;
- `leadAmtCustomerId` - links to `mart_accounts.customerId` when a customer has offcially been created agisnt the lead. The Customer can be either onverted or not. For a Converted customer/ lead the `mart_accounts.sale_date` must not be NULL and the `sale_date` is the conversion date.

### Dates

- `leadCreatedAt` - Datetime when the Lead was generated i.e., created in the system
- `leadUpdatedAt` - Last datetime the Lead was last updated

### Customer Information

- `firstName` - First Name of the Customer
- `middleName` - Middle Name of the customer
- `lastName` - Last Name of the Customer
- `name` - Full Names of the Customer
- `idNumber` - Unique Identification Number of Customer
- `mobilePhone` - Unique Mobile Number of a Customer - Same as `phoneNumber`
- `phoneNumber` - Unique Mobile Number of a Customer - same as `mobilePhone`

### Reshuffled Leads

- A reshuffled lead is one where `agentId` differs from `createdById` — the lead was reassigned after creation
- Use `isReshuffleLead = 1` as the canonical flag for reshuffled leads
- Use `agentId` for current ownership, `createdById` for attribution at point of creation

### Conversion

- `leadAmtCustomerId` links to `mart_accounts.customerId` when a customer has been created against the lead
- A converted lead = `mart_accounts.sale_date` is NOT NULL — `sale_date` is the conversion date
- A NULL `leadAmtCustomerId` means the lead has not yet been linked to a customer record

### Referrals

- For leads resulting from referrals, `referralId` is the unique identifier for the referral
- `referralType` flags the type of referral:
  - `CUSTOMER` — an existing customer referring another customer
  - `EMPLOYEE` — a SunCulture employee referring a customer
  - `IPOS` — referral through IPOS
  - `STOCKIST` — referral through a stockist
  - `THROUGH_PARTNER` — referral through a partner
  - `OTHER` — any other referral type
- For non-referred leads, `referralType` is `NULL`
- To filter referral leads only: `referralType IS NOT NULL`
- To filter non-referral leads: `referralType IS NULL`

### Canonical Base Query

```sql
WITH
mart_leads_cte as (
  select *
  from (
    SELECT *,
      row_number() over (partition by leadId ORDER BY _generated_at desc) as rnk
    FROM marts.mart_leads
  ) where rnk = 1
)
select *
from mart_leads_cte
```
