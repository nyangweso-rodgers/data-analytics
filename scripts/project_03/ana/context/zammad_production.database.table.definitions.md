## Database: `zammad_production`

### Purpose

The `zammad_production` database contains ticketing data adminstredt through a pltform called SunDesk. Tickets on customer complaints, issues, aftsresales e.t., used for reproting of all tickets realted KPIs and Metrics.

## Tables

### `zammad_production.tickets`

- Raw data for all logged tickets with datetime metadata, and ticket diepsositon i.e., `type`

#### Key Identifiers

- `id` - Unique ticket id
- `number` - Unique ticket number
- `title` - Ticket Title as recorded in the front end SunDesk application
- `customer_id` - AMT Customer Id. Joins to
  - `amt.customers` on `amt.customers.id`
  - `marts.mart_accounts` on `customerId`
- `first_response_at` -
- `first_response_escalation_at` -
- `first_response_in_min` -
- `first_response_diff_in_min` -
- `close_at` - DateTime when ticken was closed
- `close_escalation_at` -
- `close_in_min` -
- `close_diff_in_min` -
- `update_escalation_at` -
- `update_in_min` -
- `update_diff_in_min` -
- `last_close_at` -
- `type` - This is the ticket disposition i.e., thmeme, it inclides `Payment Procedure`, `Installation`, `Pump failure`, `Aftersales`., e.t.c.

- `created_at` - DateTime when ticket was created
- `escalation_at` - DateTime when ticket was escalated

#### Notes

#### Canonical Base Query

```sql
WITH
tickets_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk
        FROM zammad_production.tickets
        ) where rnk = 1
    )
select *
from tickets_cte
```

### `zammad_production.ticket_states`

- This for ticket states whether `NEW`, `CLOSED`

#### Key Identifiers

- `id` - Unique ticket state identifier. Joins to `zammad_production.tickets`
- `name` - Ticket states e.g., `CLOSED`, `NEW`, `IN PROGRESS`, e.t.c.,

#### Notes

#### Canonical Base Query

```sql
WITH
ticket_states_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk
        FROM zammad_production.ticket_states
        ) where rnk = 1
    )
select *
from ticket_states_cte
```
