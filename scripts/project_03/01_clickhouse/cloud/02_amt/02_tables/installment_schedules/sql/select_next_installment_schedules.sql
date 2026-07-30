WITH
--------------------- Deleted Records ----------------------------------
deleted_records_audit_cte as (
    select *
    from (
        SELECT distinct id,
        recordId,
        tableName,
        row_number() OVER (partition by id ORDER BY sync_at DESC) as rnk 
        FROM amt.deleted_records_audit
        ) where rnk = 1
),
--------------------- Installment Schedules ----------------------------------
installment_schedules_cte as (
    select distinct accountId,
    id,
    paymentSequence,
    expectedDate,
    expectedAmount
    from (
        SELECT  id,
        accountId,
        paymentSequence,
        expectedAmount,
        expectedDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM amt.installment_schedules
        ) where rnk = 1 
        and id not in (select recordId from deleted_records_audit_cte where tableName = 'installment_schedules')
        and paymentSequence <> 0
    ),
next_installment_schedule_cte AS (
    SELECT accountId,
        argMin(paymentSequence, expectedDate) AS next_payment_sequence,
        argMin(expectedAmount, expectedDate) AS next_expected_amount,
        MIN(expectedDate) AS next_expected_date
    FROM installment_schedules_cte
    WHERE expectedDate >= today()
    GROUP BY accountId
)
select *
from installment_schedules_cte
--from next_installment_schedule_cte
where accountId = '2'
limit 100