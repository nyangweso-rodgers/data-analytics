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
--------------------- Installment Payments ----------------------------------
installment_payments_cte as (
    select *
    from (
        SELECT *,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.installment_payments
        ) where rnk = 1
        and id not in (select recordId from deleted_records_audit_cte where tableName = 'installment_payments')
        )
select *
from installment_payments_cte
limit 1000