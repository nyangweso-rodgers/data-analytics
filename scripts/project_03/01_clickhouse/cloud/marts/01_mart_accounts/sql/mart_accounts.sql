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
    --and companyRegion in ('kenya', 'uganda')
    --and status = 'Refunded'
    ),
--------------------- Agg - Accounts ----------------------------------
agg_accounts_cte as (
    select distinct status,
    --toStartOfMonth()
    --toYear()
    count(distinct customerId) as customer_id_count,
    count(distinct account_id) as account_id_count
    from mart_accounts_cte
    GROUP BY 1
    ORDER BY 2 desc
    ),
--------------------- Sales ----------------------------------
sales_cte as (
    select *
    from mart_accounts_cte
    where sale_date is not null
    ),
--------------------- agg - Sales ----------------------------------
agg_sales_report_cte as (
    select distinct companyRegion,
    --status,
    --category,
    product,
    --customerType,
    --region,
    --Region,
    --supervisor_name,
    --RSM,
    --date(sale_date) as sale_date,
    sum(productQty) as productQty
    from sales_cte
    where companyRegion = 'kenya'
    --where companyRegion = 'uganda'
    and date(sale_date) >= '2026-01-01'
    group BY 1,2
    ORDER BY 1, 3 desc
    ),
--------------------- Refunds ---------------------------------- 
refunds_cte as (
    select *
    from mart_accounts_cte
    WHERE (RefundDate is not null) 
    and (sale_date is null)
    ),
agg_refunds_cte as (
    select distinct status,
    count(distinct account_id) as account_id_count
    from mart_accounts_cte
    WHERE (RefundDate is not null) 
    and sale_date is null
    group by 1
    ORDER BY 2 desc
    ),
--------------------- data quality - installed accounts with missing dispatch dates ---------------------------------- 
check_missing_dispatch_dates_cte as (
    select distinct account_id,
    companyRegion,
    accountType,
    status,
    sale_date,
    dispatchDate,
    jsf_date,
    product
    from mart_accounts_cte
    where companyRegion in ('kenya', 'uganda')
    and sale_date is not null
    and jsf_date is not null
    and dispatchDate is null
),
--------------------- data quality - installed accounts with missing dispatch dates ---------------------------------- 
check_accounts_with_sale_dates_but_null_status_cte as (
    select distinct account_id,
    accountRef,
    customerId,
    device_id,
    companyRegion,
    accountType,
    status,
    sale_date,
    dispatchDate,
    jsf_date,
    product
    from mart_accounts_cte
    where companyRegion in ('kenya', 'uganda')
    and sale_date is not null
    and nullif(status, '') is null 
)
select --*
count(*), max(_generated_at)
--min(sale_date), max(sale_date)
from mart_accounts_cte
--from agg_accounts_cte
--from agg_sales_report_cte
--from refunds_cte
--from check_accounts_with_sale_dates_but_null_status_cte
--where account_id = '200'
LIMIT 10000