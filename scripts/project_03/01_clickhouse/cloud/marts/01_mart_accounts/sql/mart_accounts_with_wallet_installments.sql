WITH
--------------------- Marts - Accounts ----------------------------------
mart_accounts_cte as (
    select *
    from (
        SELECT account_id,
        accountRef,
        accountType,
        status,
        customerId,
        identification_number,
        customer_name,
        product,
        companyRegion,
        region,
        --cds1_date,
        --cds2_date,
        sale_date,
        dispatchDate,
        jsf_date,
        first_payment_date,
        last_payment_date,
        installment_amount,
        expected_payment_amount,
        row_number()over(partition by account_id ORDER BY _generated_at desc) as rnk 
    FROM marts.mart_accounts
    ) where rnk = 1
    ),
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
    installmentType,
    paymentSequence,
    expectedDate,
    expectedAmount
    from (
        SELECT distinct id,
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
        SELECT distinct id,
        accountId,
        instalmentScheduleId,
        paymentId,
        ledgerEntryId,
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
        select distinct mart_accounts_cte.account_id as account_id,
        accountRef,
        accountType as accountType,
        status as status,
        customerId,
        identification_number,
        customer_name,
        installmentType,
        installment_amount,
        expected_payment_amount,
        paymentSequence,
        expectedDate,
        paymentDate,
        expectedAmount,
        (wallet_installment_payments_cte.amountPaid - wallet_installment_payments_cte.amountRefunded) as amountPaid,
        companyRegion,
        region,
        product,
        --cds1_date,
        --cds2_date,
        sale_date,
        dispatchDate,
        jsf_date,
        first_payment_date,
        last_payment_date
        from mart_accounts_cte
        LEFT JOIN installment_schedules_cte on installment_schedules_cte.accountId = mart_accounts_cte.account_id
        left join wallet_installment_payments_cte on wallet_installment_payments_cte.instalmentScheduleId = installment_schedules_cte.id
        ) where companyRegion = 'kenya'
        --where companyRegion in ('kenya', 'uganda')
        --and accountType in ('PAYG')
        --and status in ('Complete', 'Current', 'Repossession', 'Arrears', 'Pending Repossession', 'Write Off', 'Advance', 'Repossession On Hold', 'REPOSSESSION', 'Completed') 
        --and expectedDate is not NULL
        --and product = 'Kilimo Boost'
        --and expectedDate <= '2026-04-31'
        --and status = 'Pending Repossession'
        ORDER BY account_id, paymentSequence, expectedDate, paymentDate
    ),
--------------------- Mashup ----------------------------------
agg_installment_payments_cte as (
    select *,
    CASE
        WHEN paymentSequence = 0 THEN 'Deposit'
        WHEN expectedDate < today() THEN 'Past Due'
        WHEN expectedDate = today() THEN 'Due Today'
    ELSE 'Upcoming' END AS installment_timeline,
    CASE
        WHEN amountPaid >= expectedAmount THEN dateDiff('day', expectedDate, paymentDate)
    END AS days_to_full_payment, -- How many days late was the installment when it was eventually completed?
    CASE
        WHEN amountPaid < expectedAmount
            AND expectedDate < today()
            THEN dateDiff('day', expectedDate, today())
    ELSE 0 END AS current_dpd, -- If it's still outstanding, how many days overdue is it today?
    CASE
        WHEN ifNull(amountPaid, 0) = 0 THEN 'Unpaid'
        WHEN ifNull(amountPaid, 0) < expectedAmount THEN 'Partially Paid'
    ELSE 'Paid in Full' END AS payment_status,
    CASE
        WHEN paymentSequence <> 1 THEN NULL
        WHEN expectedDate > today() THEN NULL
        WHEN amountPaid >= expectedAmount AND paymentDate <= expectedDate THEN 0
    ELSE 1 END AS is_fpd,
    CASE
        WHEN paymentSequence <> 0 AND installment_timeline = 'Past Due'
            AND (
                payment_status <> 'Paid in Full'
                OR days_to_full_payment > 0
            )
        THEN 1
    ELSE 0 END AS arrears_flag,
    sum(CASE WHEN paymentSequence > 0 THEN expectedAmount ELSE 0 END) OVER (
        PARTITION BY account_id
        ORDER BY paymentSequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_expected_amount,
    sum(CASE WHEN paymentSequence > 0 THEN amountPaid ELSE 0 END) OVER (
        PARTITION BY account_id
        ORDER BY paymentSequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_amount_paid,
    sum(CASE WHEN paymentSequence > 0 THEN expectedAmount ELSE 0 END) OVER (
        PARTITION BY account_id ORDER BY paymentSequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) -
    sum(CASE WHEN paymentSequence > 0 THEN amountPaid ELSE 0 END) OVER (
        PARTITION BY account_id ORDER BY paymentSequence
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_arrears
    from (
        select distinct account_id,
        accountRef,
        accountType,
        status,
        customerId,
        identification_number,
        customer_name,
        companyRegion,
        region,
        product,
        date(sale_date) as sale_date,
        date(dispatchDate) as dispatchDate,
        date(jsf_date) as jsf_date,
        first_payment_date,
        last_payment_date,
        installmentType,
        installment_amount,
        expected_payment_amount,
        paymentSequence,
        expectedDate,
        max(date(paymentDate)) as paymentDate,
        expectedAmount,
        coalesce(sum(amountPaid),0) as amountPaid
        from installment_payments_mashup_cte
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22
    )
    ORDER BY account_id, paymentSequence, expectedDate, paymentDate
    ),
--------------------- arrears array calc ----------------------------------
arrears_calc_cte as (
    select
        account_id,
        paymentSequence,
        true_cumulative_arrears
    from (
        select
            account_id,
            arrayMap(x -> x.1, arr) as seq_arr,
            arrayCumSumNonNegative(arrayMap(x -> x.2, arr)) as cum_arr
        from (
            select
                account_id,
                arraySort(
                    x -> x.1,
                    groupArray(tuple(
                        paymentSequence,
                        CASE WHEN paymentSequence > 0 THEN expectedAmount ELSE 0 END - amountPaid
                    ))
                ) as arr
            from agg_installment_payments_cte
            where paymentSequence > 0
            group by account_id
        )
    )
    ARRAY JOIN seq_arr as paymentSequence, cum_arr as true_cumulative_arrears
),

--------------------- agg - collections ----------------------------------
agg_collections_rates_cte as (
    select
        t.*,
        CASE WHEN t.paymentSequence > 0 THEN t.expectedAmount ELSE 0 END AS expected_current,
        ifNull(
            LAG(c.true_cumulative_arrears) OVER (PARTITION BY t.account_id ORDER BY t.paymentSequence),
            0
        ) AS expected_arrears,
        LEAST(t.amountPaid, expected_arrears) AS collection_arrears,
        LEAST(
            GREATEST(t.amountPaid - expected_arrears, 0),
            expected_current
        ) AS collection_current,
        GREATEST(t.amountPaid - expected_arrears - expected_current, 0) AS collection_advance
    from agg_installment_payments_cte t
    LEFT JOIN arrears_calc_cte c
        ON t.account_id = c.account_id AND t.paymentSequence = c.paymentSequence
    ORDER BY t.account_id, t.paymentSequence, t.expectedDate, t.paymentDate
)
--------------------- agg - collections ----------------------------------
/*
agg_collections_rates_cte as (
    select *,
    -- expected_arreas,
    -- expeted_current
    -- collected_arrears,
    -- collected_current,
    -- collected_advance
    from agg_installment_payments_cte
    ORDER BY account_id, paymentSequence, expectedDate, paymentDate
),
*/
--------------------- check - fpds ----------------------------------
/*
check_accounts_fpd_cte as (
    select *
    from agg_installment_payments_cte
    where paymentSequence  = 1 
    and is_fpd = 1
    and companyRegion = 'uganda'
    and status not in ('Partial Deposit', 'No Deposit', 'Full Deposit', 'Pending Installation')
    ),
*/ 
--------------------- check - accounts in arrears ----------------------------------
/*
check_accounts_ever_in_arrears_cte as (
    select distinct companyRegion,
    account_id,
    max(arrears_flag) as ever_in_arrears  -- 1 if ANY installment was in arrears, else 0
    from agg_installment_payments_cte
    where paymentSequence <> 0
    group by companyRegion, account_id
),
*/
--------------------- agg - accounts in arrears ----------------------------------
/*
agg_accounts_ever_in_arrears_cte as (
    select distinct companyRegion,
    count(distinct account_id) as total_accounts,
    countIf(ever_in_arrears = 1) as accounts_ever_in_arrears,
    countIf(ever_in_arrears = 0) as accounts_never_in_arrears
    from check_accounts_ever_in_arrears_cte
    group by companyRegion
),
*/
--------------------- account-level max aging ----------------------------------
/*
check_accounts_max_aging_cte as (
    select
        companyRegion,
        account_id,
        max(
            greatest(
                coalesce(current_dpd, 0),
                coalesce(days_to_full_payment, 0)
            )
        ) as max_aging_days
    from agg_installment_payments_cte
    where paymentSequence <> 0
    group by companyRegion, account_id
),
*/
--------------------- check - accounts with null expectedDate ----------------------------------
/*
check_accounts_with_null_expected_dates_cte as (
    select distinct account_id,
    accountRef,
    identification_number,
    companyRegion,
    accountType,
    status,
    sale_date,
    dispatchDate,
    jsf_date
    --count(distinct account_id) as total_accounts
    from agg_installment_payments_cte
    where accountType in ('PAYG')
    and paymentSequence <> 0
    and expectedDate is NULL
    and jsf_date is not NULL
    and companyRegion = 'uganda'
    and status not in ('Partial Deposit', 'No Deposit', 'Full Deposit', 'Pending Installation')
    ),
*/
--------------------- agg - accounts with null expectedDate ----------------------------------
/*
agg_accounts_with_null_expected_dates_cte as (
    select distinct companyRegion,
    status,
    count(distinct account_id) as total_accounts
    from check_accounts_with_null_expected_dates_cte
    group by 1,2
    order by 1, 3 desc
    ),
*/
--------------------- check - accounts with first expectedDate in 2027 ----------------------------------
/*
check_accounts_with_first_expected_date_in_2027_cte as (
    select distinct account_id,
    accountRef,
    identification_number,
    companyRegion,
    accountType,
    status,
    sale_date,
    dispatchDate,
    jsf_date,
    first_payment_date,
    last_payment_date,
    paymentSequence,
    expectedDate
    from agg_installment_payments_cte
    where accountType in ('PAYG')
    and paymentSequence = 1
    and toYear(expectedDate) = '2027'
    and jsf_date is not NULL
    and companyRegion = 'uganda'
    and status not in ('Partial Deposit', 'No Deposit', 'Full Deposit', 'Pending Installation') 
),
*/
--------------------- 70-day aging threshold summary ----------------------------------
/*
agg_accounts_max_aging_cte as (
    select
        companyRegion,
        count(distinct account_id) as total_accounts,
        countIf(max_aging_days > 70) as accounts_exceeded_70_days_ever,   -- <-- this answers your new question
        countIf(max_aging_days <= 70) as accounts_never_exceeded_70_days  -- <-- this answers the previous one
    from check_accounts_max_aging_cte
    group by companyRegion
),
*/
--------------------- agg -Payment Sequence ----------------------------------
/*
check_payment_sequence_cte as (
    select distinct paymentSequence,
    count(*)
    from (SELECT distinct account_id,
    max(paymentSequence) as paymentSequence
    from agg_installment_payments_cte
    where product = 'Kilimo Boost'
    group by 1
    ) group by 1
    order by 1
    )
*/
select *
--count(distinct account_id)
--from mashup_cte
--from check_accounts_fpd_cte
--from agg_accounts_cte
--from agg_arrears_summary_cte
--from agg_installment_payments_cte
--from agg_accounts_ever_in_arrears_cte
--from agg_accounts_max_aging_cte
--from check_fpd_accounts_cte
--from check_accounts_with_null_expected_dates_cte
--from agg_accounts_with_null_expected_dates_cte
--from check_accounts_with_first_expected_date_in_2027_cte
--from accounts_has_ever_been_in_arrears_cte
--from ad_hoc_request_cte
--from agg_cte
--from check_payment_sequence_cte
--from arrears_calc_cte
from agg_collections_rates_cte
--where account_id in ('164948')
--where accountRef = 'CF84029102NGWH'
--where (paymentSequence <> 0) and (installment_timeline = 'Past Due') and (days_to_full_payment <= 0)
--where account_id in (select distinct account_id from check_accounts_ever_in_arrears_cte where ever_in_arrears = 0)
--where has_ever_been_in_arrears = 0
--where account_id = '86448'
--where identification_number = 'CM60007100LAWG'
--where accountRef = '36463428'
--where account_id = '142655' # check out this
where account_id = '1128'
--ORDER BY account_id
limit 1000