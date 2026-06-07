
--CREATE VIEW `sales-service`.vw_leads_tunda AS
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
        firstName,
        lastName,
        leadCategory,
        referralId,
        productOfInterest,
        is_migrated,
        row_number() OVER (partition by leadId ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.leads
    ) WHERE rnk = 1
    )
select *
from leads_cte