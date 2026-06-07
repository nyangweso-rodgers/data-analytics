WITH
--------------------- CDS ----------------------------------
cds_cte as (
    select *
    from (
        SELECT distinct id,
        leadId,
        --customerId, # all NULL
        --accountId,# all NULL
        --mobileNumber, # all NULL
        cds1CompletionDate,
        cds2CompletionDate,
        creditScore,
        creditCheckStatus,
        cdsId,
        --creditCheckStatus,
        --creditScore,
        --creditScoreDate,
        --stage,
        --creditReviewStatus, # all 1
        is_migrated,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        --FROM sunculture.cds
        FROM `sales-service`.cds
        ) where rnk = 1
    ),
cds_summary_cte as (
    Select distinct leadId,
    cds1CompletionDate as CDS1_Date__c,
    cds2CompletionDate as CDS2_Date__c,
    creditScore AS New_Credit_Score__c,
    creditCheckStatus AS New_Credit_Check_Result_Status__c
    from cds_cte
    )
select *
from cds_cte
--where leadId = '00Q8d000009PQBcEAO'
where leadId = '01KH69VR0XRYAHJ71XR4RMGM88'
limit 1000