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
        product,
        companyRegion,
        jsf_date,
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
        accountRef,
        accountType as accountType,
        status as status,
        customerId,
        installment_schedules_cte.installmentType as installmentType,
        installment_schedules_cte.paymentSequence as paymentSequence,
        installment_schedules_cte.expectedDate as expectedDate,
        wallet_installment_payments_cte.paymentDate as paymentDate,
        installment_schedules_cte.expectedAmount as expectedAmount,
        (wallet_installment_payments_cte.amountPaid - wallet_installment_payments_cte.amountRefunded) as amountPaid,
        companyRegion,
        mart_accounts_cte.jsf_date as jsf_date
        from mart_accounts_cte
        LEFT JOIN installment_schedules_cte on installment_schedules_cte.accountId = mart_accounts_cte.account_id
        left join wallet_installment_payments_cte on wallet_installment_payments_cte.instalmentScheduleId = installment_schedules_cte.id
        ) where companyRegion in ('kenya', 'uganda')
        and accountType in ('PAYG')
        --and status not in ('No Deposit', 'Full Deposit', 'Refunded')
        --and status = 'Pending Repossession'
        --and status in ('Complete', 'Current', 'Repossession', 'Arrears', 'Pending Repossession', 'Write Off', 'Advance', 'Repossession On Hold', 'REPOSSESSION', 'Completed') 
        --and expectedDate is not NULL
        --and expectedDate <= '2026-04-31'
        --and product = 'Kilimo Boost'
        and accountId = '72118' # first paymentDate < jsf_date
        ORDER BY accountId, paymentSequence, expectedDate, paymentDate
    ),
--------------------- check - Installment Payments Before JSF Date ----------------------------------
/*
check_installment_payments_before_jsf_date_cte as (
    select *
    --count(distinct accountId) as account_id_count
    from (
        select distinct accountId,
        accountRef,
        jsf_date,
        min(date(paymentDate)) as first_wallet_installment_payment_date
        from installment_payments_mashup_cte
        where paymentSequence = 1
        group by 1,2,3
        ) where first_wallet_installment_payment_date < jsf_date
),
*/
--------------------- Account-level dimensions, one row per account ----------------------------------
account_dim_cte AS (
    select *,
    toStartOfMonth(date(jsf_date)) AS jsf_month
    from (
        SELECT accountId as accountId,
        any(accountRef) AS accountRef,
        any(accountType) AS accountType,
        any(status) AS status,
        any(customerId) AS customerId,
        any(date(jsf_date)) AS jsf_date
    FROM installment_payments_mashup_cte
    GROUP BY accountId
    )
),
--------------------- Earliest actual collection month per account ----------------------------------
agg_collections_cte AS (
    SELECT
        accountId,
        toStartOfMonth(min(paymentDate)) AS first_payment_month
    FROM installment_payments_mashup_cte
    WHERE paymentSequence >= 1
    and paymentDate IS NOT NULL
    GROUP BY accountId
),
--------------------- Spine start = earliest of jsf_month or actual first payment; jsf_month rides along as an attribute ----------------------------------
account_spine_start_cte AS (
    SELECT
        account_dim_cte.accountId,
        account_dim_cte.jsf_month,
        least(account_dim_cte.jsf_month, coalesce(agg_collections_cte.first_payment_month, account_dim_cte.jsf_month)) AS min_month
    FROM account_dim_cte
    LEFT JOIN agg_collections_cte ON account_dim_cte.accountId = agg_collections_cte.accountId
),
-------------------- Generate the monthly spine per account ----------------------------------
account_months_cte AS (
    SELECT
        accountId,
        jsf_month,
        addMonths(min_month, number) AS month
    FROM account_spine_start_cte
    ARRAY JOIN range(
        dateDiff('month', min_month, toStartOfMonth(today())) + 1
    ) AS number
),
--------------------- Dedupe to schedule grain (avoid double-counting expectedAmount from the 1:many payment join) ----------------------------------
agg_monthly_expected_cte AS (
    select accountId,
    toStartOfMonth(date(expectedDate)) AS expected_month,
    sum(expectedAmount) AS expectedAmount
    from (
        SELECT DISTINCT accountId,
        expectedDate,
        expectedAmount
    FROM installment_payments_mashup_cte
    ) GROUP BY accountId, expected_month
),
--------------------- Aggregate actual collections by accountId + month ----------------------------------
agg_monthly_wallet_installment_payments_cte AS (
    SELECT accountId,
    toStartOfMonth(date(paymentDate)) AS payment_month,
    sum(amountPaid) AS amountPaid
    FROM installment_payments_mashup_cte
    WHERE paymentSequence >= 1
    and paymentDate IS NOT NULL
    GROUP BY accountId, payment_month
    ORDER BY accountId, payment_month
),
--------------------- monthly credit history - raw ----------------------------------
credit_history_raw_cte AS (
    SELECT account_months_cte.accountId as accountId,
    account_dim_cte.accountRef,
    account_dim_cte.accountType,
    account_dim_cte.status,
    account_dim_cte.jsf_date,
    account_months_cte.jsf_month as jsf_month,
    account_months_cte.month as month,
    coalesce(agg_monthly_expected_cte.expectedAmount, 0) AS expectedAmount,
    coalesce(agg_monthly_wallet_installment_payments_cte.amountPaid, 0) AS amountPaid
    FROM account_months_cte
    LEFT JOIN agg_monthly_wallet_installment_payments_cte ON account_months_cte.accountId = agg_monthly_wallet_installment_payments_cte.accountId AND account_months_cte.month = agg_monthly_wallet_installment_payments_cte.payment_month
    LEFT JOIN agg_monthly_expected_cte
        ON account_months_cte.accountId = agg_monthly_expected_cte.accountId
        AND account_months_cte.month = agg_monthly_expected_cte.expected_month
    LEFT JOIN account_dim_cte ON account_months_cte.accountId = account_dim_cte.accountId
),
--------------------- monthly credit history - summary ----------------------------------
credit_history_summary_cte AS (
    SELECT
        *,
        greatest(prior_balance, 0) AS expected_arrears,
        greatest(expectedAmount + least(prior_balance, 0), 0) AS expectedCurrent,
        greatest(
            amountPaid - (
                greatest(prior_balance, 0)
                + greatest(expectedAmount + least(prior_balance, 0), 0)
            ), 0
        ) AS advancePayment,
        (prior_balance + expectedAmount - amountPaid) AS running_balance,   -- carries into next month's prior_balance
        sum(amountPaid) OVER (
            PARTITION BY accountId ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cum_amount_paid
    FROM (
        SELECT
            *,
            coalesce(
                sum(expectedAmount - amountPaid) OVER (
                    PARTITION BY accountId ORDER BY month
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0
            ) AS prior_balance
        FROM credit_history_raw_cte
    )
    ORDER BY accountId, month
)

select *
--from credit_history_raw_cte
from credit_history_summary_cte
limit 1000