WITH
--------------------- Tickets ----------------------------------
tickets_cte as (
    SELECT *,
     row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
    FROM zammad_production.tickets
    ),
--------------------- Validate Duplicate Tickets ----------------------------------
validate_duplicate_tickets_cte as (
    select distinct id, count(*) as xx
    from tickets_cte
    group by 1 having xx > 1
),
--------------------- Agg Tickets ----------------------------------
agg_tickets_cte as (
    select distinct 
    --type,
    inquiry_type,
    --inquiry_type,
    count(distinct id) as id_count
    from tickets_cte
    group by 1
    ORDER BY 2 desc
    ),
--------------------- Tickets Mashup ----------------------------------
tickets_mashup_cte as (
    select distinct tickets_cte.id as id, 
    tickets_cte.account as account,
    deviceid as deviceid,
    created_at as created_at,
    updated_at as updated_at,
    escalation_at as escalation_at,
    close_at as close_at
    from tickets_cte
    )
select *
--distinct type,
--count(id) as recordCount
--from tickets_cte
--from agg_tickets_cte
from tickets_mashup_cte
--where id = '57651'
--ORDER BY 2 desc
limit 1000