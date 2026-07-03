with
--------------------- View - accounts_mv ----------------------------------
accounts_mv_cte as (
    select *
    from marts.accounts_mv
),
--------------------- Agg - Account Status ----------------------------------
agg_account_status_cte as (
    select distinct 
    --status,
    region,
    County,
    count(distinct account_id) as account_id_count
    from accounts_mv_cte
    where companyRegion = 'kenya'
    group by 1,2
    ORDER BY 1,2
),
--------------------- Agg - Daily Sales ----------------------------------
agg_daily_sales_cte as (
    SELECT distinct companyRegion,
    date(sale_date) as sale_date,
    sum(productQty) as productQty
    FROM accounts_mv_cte
    --where date(sale_date) BETWEEN '2026-03-01' and '2026-03-13'
    where companyRegion = 'kenya'
    --and sale_date
    GROUP BY 1,2
    ORDER BY companyRegion, sale_date DESC
    )
select *
--from accounts_mv_cte
--from agg_daily_sales_cte
from agg_account_status_cte
--where account_id = '100228'
LIMIT 1000