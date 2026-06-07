WITH
--------------------- Leads ----------------------------------
leads_cte as (
    select *
    from (
        SELECT createdAt,
        updatedAt,
        --sync_at,
        companyRegionId,
        leadId,
        mobilePhone,
        idNumber,
        leadAmtCustomerId,
        leadSourceId,
        leadChannelId,
        createdById,
        agentId,
        is_migrated,
        row_number() OVER (partition by leadId ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.leads
    ) WHERE rnk = 1
    ),
--------------------- Lead Sources ----------------------------------
leadsources_cte as (
    select *
    from (
        SELECT id,
        name,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM `sales-service`.leadsources
        ) where rnk = 1
    ),
--------------------- Lead Channels ----------------------------------
lead_channels_cte as (
    select *
    from (
        SELECT id,
        name,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk   
        FROM `sales-service`.lead_channels 
        ) where rnk = 1
    ),
--------------------- Dupolicate Leads ----------------------------------
duplicate_leads_cte as (
    select distinct leadId, count(*) as lead_id_count
    from leads_cte
    GROUP BY 1
    having lead_id_count > 1
    ORDER BY 2 desc
    ),
--------------------- Lead Mashup ----------------------------------
leads_mashup_cte as (
    select distinct 
    leads_cte.leadId as leadId,
    leads_cte.idNumber as idNumber,
    leads_cte.createdAt as createdAt,
    companyRegionId,
    leads_cte.mobilePhone as mobilePhone,
    lead_channels_cte.name as leadChannel,
    leadsources_cte.name as leadsource,
    leadAmtCustomerId,
    createdById,
    agentId,
    is_migrated as leadIsMigrated
    from leads_cte
    left join leadsources_cte on leadsources_cte.id = leads_cte.leadSourceId
    left join lead_channels_cte on toInt64(lead_channels_cte.id) = leads_cte.leadChannelId
    )
select --*
distinct leadChannel, leadsource
--max(createdAt), count(distinct leadId)
from leads_mashup_cte
--where leadAmtCustomerId in ('5205', '47106', '47108')
--where idNumber in ()
where companyRegionId = 1
and leadsource = 'Door to Door'
--and leadId in ()
--group by 1,2 
order by 1,2