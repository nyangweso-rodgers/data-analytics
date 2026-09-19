WITH
--------------------- Marts - Accounts ----------------------------------
mart_accounts_cte as (
    select *
    from (
        SELECT *,
    row_number()over(partition by account_id ORDER BY _generated_at desc) as rnk 
    FROM marts.mart_accounts
    ) where rnk = 1
    --and companyRegion = 'kenya'
    --and accountType in ('PAYG')
    --and companyRegion in ('kenya', 'uganda')
    --and status = 'Refunded'
    --and accountRef = '24579874'
    --and identification_number = ''
    --and accountRef = '24213241'
    and account_id = '142705'
    ),
--------------------- Payments ----------------------------------
payments_cte as (
    SELECT *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.payments
        ) where rnk = 1
        ),
--------------------- Payments ----------------------------------
agg_customer_payments_cte as (
    SELECT distinct customerId,
    timestamp_made,
    sum(amount) as amount
    FROM payments_cte
    group by 1, 2
    ),
--------------------- Marts - Accounts ----------------------------------
mart_accounts_with_payments_cte as (
    select distinct mart_accounts_cte.account_id as accountId 
    from mart_accounts_cte
    )
select *
from mart_accounts_cte
--from mart_accounts_with_payments_cte
limit 1000