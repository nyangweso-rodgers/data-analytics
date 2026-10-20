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
    ),
--------------------- zammad_production - organizations ----------------------------------
organizations_cte as (
    select *
    from (
        SELECT id, name,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
        FROM zammad_production.organizations
        ) where rnk = 1
    ),
--------------------- zammad_production - groups ----------------------------------
groups_cte as (
    select *
    from (
        SELECT id, name,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk  
        FROM zammad_production.groups
        ) where rnk = 1
    ),
--------------------- zammad_production -  ----------------------------------
ticket_states_cte as (
    select *
    from (
        SELECT id, name,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk
        FROM zammad_production.ticket_states
        ) where rnk = 1
        ), 
---------------------zammad_production - dispositions ----------------------------------
dispositions_cte as (
    select *
    from (
        SELECT id, name,
        row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
        FROM zammad_production.dispositions
        ) where rnk = 1
    ),
--------------------- users ----------------------------------
users_cte AS (
    SELECT *
    FROM (
        SELECT *,
        row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rnk
        FROM zammad_production.users
        ) WHERE rnk = 1
    ),
--------------------- tickets_mashup_cte ----------------------------------
tickets_mashup_cte as (
    select *
    from (
        select tickets_cte.id as id,
        tickets_cte.number as number,
        tickets_cte.owner_id as owner_id,
        --owners_cte.firstname as firstname,
        --owners_cte.lastname as lastname,
        concat(coalesce(owners_cte.firstname, ''), ' ', coalesce(owners_cte.lastname, '')) AS full_name,
        tickets_cte.customer_id as customer_id,
        account as account,
        nullIf(tickets_cte.deviceid, '') as deviceid,
        nullIf(organizations_cte.name, '') as organization_name,
        nullIf(groups_cte.name, '') as ticket_group,
        dispositions_cte.name as disposition_name,
        tickets_cte.type as type,
        tickets_cte.title as title,
        inquiry_type as inquiry_type,
        ticket_states_cte.name as ticket_state,
        tickets_cte.created_at as created_at,
        tickets_cte.updated_at as updated_at,
        tickets_cte.sync_at as sync_at
        from tickets_cte
        left join organizations_cte on organizations_cte.id = tickets_cte.organization_id
        left join groups_cte on groups_cte.id = tickets_cte.group_id
        left join ticket_states_cte on ticket_states_cte.id = tickets_cte.state_id
        left join dispositions_cte on dispositions_cte.id = tickets_cte.disposition_id
        left join users_cte as owners_cte on owners_cte.id = tickets_cte.owner_id
    )
    where number = '20260304810154'
),
--------------------- agg - tickets_mashup_cte ----------------------------------
agg_tickets_mashup_cte as (
    select distinct 
    organization_name,
    ticket_group,
    --type,
    --inquiry_type,
    count(*) as record_count,
    count(distinct id) as id_count,
    count(distinct number) as number_count
    from tickets_mashup_cte
    group by 1,2
    ORDER BY 1,3 desc
    )
select *
--from tickets_cte
from tickets_mashup_cte
--from agg_tickets_mashup_cte
limit 1000