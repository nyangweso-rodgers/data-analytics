## Table: credit_score_model.ml_payg_interventions

### Purpose

Defaulter's Credit Score Model output which categorizes accounts `accountId` based on probability of default, in addition to a recommendation

### Key Identifiers

- `scored_at` - Datetime when the credit scoring was done
- `accountId` - Primary key. Unique per account.

### Canonical Base Query

```sql
WITH
ml_payg_interventions_cte as (
  select *
  from (
    SELECT *
    FROM credit_score_model.ml_payg_interventions
  )
)
select *
from ml_payg_interventions_cte
```
