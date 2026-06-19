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
--------------------- PTPs Agg ----------------------------------
agg_ptps_reasons_cte as (
    select distinct 
    --reason,
    --callStatus,
    ptpStatus,
    count(distinct id) as id_count,
    count(distinct accountId) as account_id_count
    from ptps_cte
    group by 1 
    ORDER BY 2 desc
    ),
--------------------- PTPs Agg ----------------------------------
agg_account_ptps_cte as (
    select distinct accountId as accountId,
    count(distinct id) as id_count
    from ptps_cte
    group by 1 
    HAVING id_count > 1
    ORDER BY 2 desc
    )
select * 
--count(*), max(createdAt), max(updatedAt), max(sync_at)
--from ptps_cte
from agg_ptps_reasons_cte
--from agg_account_ptps_cte
--where accountId = '74015'
LIMIT 31