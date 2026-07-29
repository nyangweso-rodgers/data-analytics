WITH
--------------------- PTPs ----------------------------------
ptps_cte as (
    select *
    from (
        SELECT *,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
    FROM amt.ptps
    ) where rnk = 1
    ORDER BY accountId, createdAt desc
    ),
--------------------- Marts - Accounts ----------------------------------
mart_accounts_cte as (
    select *
    from (
        SELECT account_id,
        accountRef,
        accountType,
        status,
        customerId,
        --identification_number,
        --product,
        companyRegion,
        region,
        --cds1_date,
        --cds2_date,
        --sale_date,
        --dispatchDate,
        --jsf_date,
        row_number()over(partition by account_id ORDER BY _generated_at desc) as rnk 
    FROM marts.mart_accounts
    ) where rnk = 1
    ),
--------------------- PTPs - Mashup  ----------------------------------
ptps_mashup_cte as (
    select *
    from (
        select distinct ptps_cte.accountId as accountId,
        ptps_cte.id as ptpId,
        ptpDate,
        ptpStatus,
        companyRegion,
        region,
        createdAt
    from ptps_cte
    left join mart_accounts_cte on mart_accounts_cte.account_id = ptps_cte.accountId
    ) where companyRegion = 'kenya'
),
--------------------- PTPs - Agg  ----------------------------------
agg_ptps_cte as (
    select distinct toStartOfMonth(date(createdAt)) as month,
    companyRegion,
    region,
    count(distinct ptpId) as ptp_requests_count,
    count(distinct accountId) as ptp_accounts_count
    from ptps_mashup_cte
    --where date(createdAt) >= '2026-01-01'
    GROUP BY 1,2,3
    ORDER BY 1 desc, 3 desc
    )
select *
--from ptps_mashup_cte
from agg_ptps_cte
LIMIT 31