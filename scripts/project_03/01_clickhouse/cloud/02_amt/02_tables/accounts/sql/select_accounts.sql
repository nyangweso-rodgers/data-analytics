WITH
--------------------- Accounts ----------------------------------
accounts_cte as (
    select *
    from (
        SELECT id,
        createdAt,
        updatedAt,
        customerId,
        accountRef,
        status,
        fullDepositDate,
        accountBalance,
        updatedAt,
        sync_at,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.accounts
        ) where rnk = 1
        )
select --*
count(*), max(updatedAt), max(sync_at)
--count(*) as record_count, count(distinct id) as account_id_count
--distinct status, count(distinct id)
from accounts_cte
--where accountRef in ('21115497')
--where id in ('143280')
--ORDER BY customerId, id
--GROUP BY 1 ORDER BY 2 DESC
limit 1000