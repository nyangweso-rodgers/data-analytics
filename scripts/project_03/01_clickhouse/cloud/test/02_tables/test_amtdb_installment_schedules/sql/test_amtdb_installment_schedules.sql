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
test_amtdb_installment_schedules_cte as (
    select *
    from (
        SELECT id,
        accountId,
        customerId,
        installmentType,
        paymentSequence,
        expectedAmount,
        expectedDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM test.test_amtdb_installment_schedules
        ) where rnk = 1 
        --and id not in (select recordId from deleted_records_audit_cte where tableName = 'installment_schedules')
        --and paymentSequence <> 0
        ORDER BY customerId, accountId, paymentSequence
    )
select *
--count(*) as record_count, count(distinct accountId) as account_id_count
from test_amtdb_installment_schedules_cte
--where id = '451954'
where accountId = '72118'
LIMIT 1000