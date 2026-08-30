WITH
--------------------- Marts - Accounts ----------------------------------
mart_accounts_cte as (
    select *
    from (
        SELECT account_id,
        accountType,
        status,
        companyRegion,
        row_number()over(partition by account_id ORDER BY _generated_at desc) as rnk 
    FROM marts.mart_accounts
    ) where rnk = 1
    ),
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
        amountPaid,
        amountRefunded,
        paymentDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.wallet_installment_payments
        ) where rnk = 1
        and id not in (select recordId from deleted_records_audit_cte where tableName = 'wallet_installment_payments')
    ),
--------------------- Mashup ----------------------------------
installment_payments_mashup_cte as (
    select *
    from (
        select distinct mart_accounts_cte.account_id as accountId,
        companyRegion,
        accountType as accountType,
        nullIf(mart_accounts_cte.status, '') as status,
        installment_schedules_cte.paymentSequence as paymentSequence,
        installment_schedules_cte.expectedDate as expectedDate,
        paymentDate,
        expectedAmount,
        (wallet_installment_payments_cte.amountPaid - wallet_installment_payments_cte.amountRefunded) as amountPaid
        from mart_accounts_cte
        LEFT JOIN installment_schedules_cte on installment_schedules_cte.accountId = mart_accounts_cte.account_id
        left join wallet_installment_payments_cte on wallet_installment_payments_cte.instalmentScheduleId = installment_schedules_cte.id
        ) where accountType in ('PAYG')
        and status not in ('No Deposit', 'Full Deposit', 'Refunded', 'Rejected', 'No Deposit', 'Partial Refunded', 'Partial Deposit', 'No Deposit')
        and companyRegion = 'kenya'
        ORDER BY accountId, paymentSequence, expectedDate, paymentDate
    ),
--------------------- Mashup ----------------------------------
agg_installment_payments_cte as (
    select *,
    CASE
        WHEN paymentSequence <> 1 THEN NULL
        WHEN expectedDate > today() THEN NULL
        WHEN amountPaid >= expectedAmount AND paymentDate <= expectedDate THEN 0
    ELSE 1 END AS is_fpd,
    CASE
        WHEN paymentSequence <> 1 THEN NULL
        WHEN expectedDate > today() THEN NULL
        WHEN amountPaid >= expectedAmount THEN 0
        ELSE 1
    END AS is_fpd_amount
    from (
        select distinct accountId,
        companyRegion,
        status,
        accountType,
        paymentSequence,
        expectedDate,
        max(date(paymentDate)) as paymentDate,
        expectedAmount,
        coalesce(sum(amountPaid),0) as amountPaid
        from installment_payments_mashup_cte
        GROUP BY 1,2,3,4,5,6,8
    )
    ORDER BY accountId, paymentSequence, expectedDate, paymentDate
    ),
--------------------- features - schediues summary ----------------------------------
agg_account_installment_schedule_summary_cte AS (
    select *,
    CASE
        WHEN is_fpd_amount = 1 THEN dateDiff('day', first_expected_date, today())
        ELSE NULL
    END AS fpd_amount_days_late
    from (
        SELECT accountId,
        maxIf(expectedDate, paymentSequence = 1) AS first_expected_date,
        maxIf(is_fpd, paymentSequence = 1) AS is_fpd,
        maxIf(is_fpd_amount, paymentSequence = 1) AS is_fpd_amount,
        max(paymentSequence) as installment_schedules_count,
        argMinIf(paymentSequence, expectedDate, expectedDate >= today()) AS next_payment_sequence,
        argMinIf(expectedAmount, expectedDate, expectedDate >= today()) AS next_expected_amount,
        minIf(expectedDate, expectedDate >= today()) AS next_expected_date,
        argMaxIf(paymentSequence, expectedDate, expectedDate < today()) AS current_payment_sequence,
        argMaxIf(expectedAmount, expectedDate, expectedDate < today()) AS current_expected_amount,
        maxIf(expectedDate, expectedDate < today()) AS current_expected_date
        FROM agg_installment_payments_cte
        where paymentSequence <> 0
        and expectedDate is not null
        GROUP BY accountId
    )
    )
select *
from agg_account_installment_schedule_summary_cte
--where accountId = '109768'
limit 1000