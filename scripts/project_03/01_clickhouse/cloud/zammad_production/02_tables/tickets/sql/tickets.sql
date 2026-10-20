WITH
--------------------- zammad_production - tickets ----------------------------------
tickets_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
        FROM zammad_production.tickets
        ) where rnk = 1
        --and date(created_at) = '2026-07-15'
        and number = '20260626810172'
    ),
--------------------- Agg Tickets ----------------------------------
agg_tickets_cte as (
    select distinct 
    organization_id,
    --type,
    --inquiry_type,
    --inquiry_type,
    count(*) as record_count,
    count(distinct id) as id_count
    from tickets_cte
    group by 1
    ORDER BY 2 desc
    )
select *
from tickets_cte
--from agg_tickets_cte
--where id = '57651'
--ORDER BY note desc
limit 1000