WITH
--------------------- Marts - Accounts ----------------------------------
mart_accounts_cte as (
    select *
    from (
        SELECT *,
    row_number()over(partition by account_id ORDER BY _generated_at desc) as rnk 
    FROM marts.mart_accounts
    ) where rnk = 1
    and companyRegion = 'kenya'
    and accountType in ('PAYG')
    --and companyRegion in ('kenya', 'uganda')
    --and status = 'Refunded'
    --and accountRef = '24579874'
    --and identification_number = ''
    ),
--------------------- Agg - Accounts ----------------------------------
/*
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
*/
--------------------- agg - Sales ----------------------------------
/*
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
    from mart_accounts_cte
    where companyRegion = 'kenya'
    --where companyRegion = 'uganda'
    and date(sale_date) >= '2026-01-01'
    group BY 1,2
    ORDER BY 1, 3 desc
    ),
*/
--------------------- agg - installations ----------------------------------
agg_installations_cte as (
    select distinct companyRegion,
    toStartOfMonth(date(jsf_date)) as jsf_month,
    --date(jsf_date) as jsf_date,
    --product,
    count(*) as record_count,
    count(distinct account_id) as account_id_count,
    count(distinct customerId) as customer_id_count,
    count(distinct jsf_id) as jsf_id_count
    from mart_accounts_cte
    where jsf_type = 'INSTALLATION'
    AND engineer_recommendation = 'Installed'
    and accountType in ('PAYG')
    --WHERE jsf_date is not null
    --AND date(jsf_date) = '2026-09-02'
    and toStartOfMonth(date(jsf_date)) = '2026-07-01'
    group BY 1,2
    ORDER BY 1,2,4 desc
),
--------------------- Refunds ---------------------------------- 
/*
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
*/
--------------------- data quality - installed accounts with missing dispatch dates ---------------------------------- 
/*
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
*/
--------------------- data quality - installed accounts with missing dispatch dates ---------------------------------- 
/*
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
)*/
select *
--count(*), max(_generated_at)
--min(sale_date), max(sale_date)
--from mart_accounts_cte
from agg_installations_cte
LIMIT 10000