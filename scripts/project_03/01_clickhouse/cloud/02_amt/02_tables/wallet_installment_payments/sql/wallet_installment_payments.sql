WITH
--------------------- amt - deleted_records_audit ----------------------------------
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
--------------------- wallet_installment_payments_cte ----------------------------------
wallet_installment_payments_cte as (
    select *,
    from (
        SELECT distinct id,
        accountId,
        instalmentScheduleId,
        --ledgerEntryId,
        paymentType,
        --amountPaid,
        amountRefunded,
        (amountPaid - amountRefunded) as amountPaid,
        paymentDate,
        refundDate,
        discountId,
        --paymentId, -- mostly `null`
        --refundId,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.wallet_installment_payments
        where refundDate is null
    ) where rnk = 1
    and id not in (select recordId from deleted_records_audit_cte where tableName = 'wallet_installment_payments')
    ORDER BY accountId, instalmentScheduleId,paymentDate
    ),
--------------------- agg - wallet_installment_payments_cte ----------------------------------
agg_wallet_installment_payments_cte as (
    select distinct accountId,
    instalmentScheduleId,
    date(paymentDate) as paymentDate,
    sum(amountPaid) as amountPaid
    from wallet_installment_payments_cte
    group by 1,2,3
    ORDER BY accountId,paymentDate
)
select *
--distinct paymentType
--count(*), count(distinct id), count(distinct accountId)
--count(distinct paymentId)
--from wallet_installment_payments_cte
from agg_wallet_installment_payments_cte
--where paymentType = 0
--where accountId is null
where accountId = '108876'
--ORDER BY sync_at
--where accountId = '200'
--order by instalmentScheduleId asc
LIMIT 1000