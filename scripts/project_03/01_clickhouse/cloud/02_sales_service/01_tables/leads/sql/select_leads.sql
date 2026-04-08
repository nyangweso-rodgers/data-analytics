WITH
--------------------- Leads ----------------------------------
leads_cte as (
        select *
        from (
            SELECT distinct createdAt,
        updatedAt,
        companyRegionId,
        leadId,
        mobilePhone,
        idNumber,
        leadAmtCustomerId,
        leadSourceId,
        leadChannelId,
        createdById,
        employeeReferralId,
        referralType,
        referralId,
        agentId,
        agentProviderId,
        name,
        firstName,
        lastName,
        is_migrated,
         row_number() OVER (partition by leadId ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.leads
        --FROM `sales-service`.vw_leads_tunda
        ) where rnk = 1
    ),
duplicate_leads_cte as (
select distinct leadId, count(*) as lead_id_count
from leads_cte
GROUP BY 1
having lead_id_count > 1
ORDER BY 2 desc
)
select *
--max(createdAt), max(updatedAt), max(sync_at), count(distinct leadId), count(*)
from leads_cte
where leadId = '0013tjPCrKUXEWMae'
limit 1000