WITH
--------------------- Deleted Records ----------------------------------
deleted_records_audit_cte as (
    select *
    from (
        SELECT  id,
        recordId,
        tableName,
        row_number() OVER (partition by id ORDER BY sync_at DESC) as rnk 
        FROM amt.deleted_records_audit
        ) where rnk = 1
),
--------------------- Installment Schedules ----------------------------------
installment_schedules_cte as (
    select *
    from (
        SELECT  id,
        accountId,
        installmentType,
        paymentSequence,
        expectedAmount,
        expectedDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM amt.installment_schedules
        ) where rnk = 1 
        and id not in (select recordId from deleted_records_audit_cte where tableName = 'installment_schedules')
    ),
--------------------- Wallet Installment Payments ----------------------------------
wallet_installment_payments_cte as (
    select *
    from (
        SELECT id,
        accountId,
        instalmentScheduleId,
        paymentId,
        --ledgerEntryId,
        amountPaid,
        amountRefunded,
        paymentDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.wallet_installment_payments
        ) where rnk = 1
        and id not in (select recordId from deleted_records_audit_cte where tableName = 'wallet_installment_payments')
    ),
--------------------- mashup - Installment Schedules ----------------------------------
installment_schedules_mashup_cte as (
    select distinct installment_schedules_cte.accountId as accountId,
    installmentType as installmentType,
    paymentSequence,
    expectedDate,
    paymentDate,
    expectedAmount,
    (wallet_installment_payments_cte.amountPaid - wallet_installment_payments_cte.amountRefunded) as amountPaid
    from installment_schedules_cte 
    left join wallet_installment_payments_cte on wallet_installment_payments_cte.instalmentScheduleId = installment_schedules_cte.id
    ),
--------------------- agg - Installment Schedules ----------------------------------
agg_installment_schedules_mashup_cte as (
    select *,
    CASE
        WHEN paymentSequence <> 1 THEN NULL
        WHEN expectedDate > today() THEN NULL
        WHEN amountPaid >= expectedAmount AND paymentDate <= expectedDate THEN 0
        ELSE 1
    END AS is_fpd,
    CASE
        WHEN paymentSequence <> 1 THEN NULL
        WHEN expectedDate > today() THEN NULL
        WHEN amountPaid >= expectedAmount THEN 0
        ELSE 1
    END AS is_fpd_amount
    from (
        select distinct accountId,
        installmentType,
        paymentSequence,
        expectedDate,
        max(date(paymentDate)) as paymentDate,
        expectedAmount,
        coalesce(sum(amountPaid),0) as amountPaid
        from installment_schedules_mashup_cte
        GROUP BY 1,2,3,4,6
        )
    ORDER BY accountId, paymentSequence
    ),
--------------------- Next Installment Schedules ----------------------------------
next_installment_schedule_cte AS (
    SELECT accountId,
    argMin(paymentSequence, expectedDate) AS next_payment_sequence,
    argMin(expectedAmount, expectedDate) AS next_expected_amount,
    MIN(expectedDate) AS next_expected_date
    FROM installment_schedules_cte
    WHERE paymentSequence <> 0
    and expectedDate >= today()
    GROUP BY accountId
    ),
--------------------- Current Installment Schedule ----------------------------------
current_installment_schedule_cte AS (
    SELECT accountId,
    argMax(paymentSequence, expectedDate) AS current_payment_sequence,
    argMax(expectedAmount, expectedDate) AS current_expected_amount,
    MAX(expectedDate) AS current_expected_date
    FROM installment_schedules_cte
    WHERE paymentSequence <> 0
    and expectedDate < today()
    GROUP BY accountId
    ),
--------------------- Current + Next Installment Schedule ----------------------------------
installment_schedule_summary_cte AS (
    SELECT accountId,
    CASE
        WHEN paymentSequence = 1 THEN expectedDate
    ELSE NULL END AS first_expected_date,
    is_fpd,
    is_fpd_amount,
    max(paymentSequence) as installment_schedules_count,
    argMinIf(paymentSequence, expectedDate, expectedDate >= today()) AS next_payment_sequence,
    argMinIf(expectedAmount, expectedDate, expectedDate >= today()) AS next_expected_amount,
    minIf(expectedDate, expectedDate >= today()) AS next_expected_date,
    argMaxIf(paymentSequence, expectedDate, expectedDate < today()) AS current_payment_sequence,
    argMaxIf(expectedAmount, expectedDate, expectedDate < today()) AS current_expected_amount,
    maxIf(expectedDate, expectedDate < today()) AS current_expected_date
    FROM agg_installment_schedules_mashup_cte
    WHERE paymentSequence <> 0
    and expectedDate is not null
    GROUP BY accountId, first_expected_date, is_fpd, is_fpd_amount
    )
select *
--from installment_schedules_mashup_cte
--from agg_installment_schedules_mashup_cte
from installment_schedule_summary_cte
limit 1000