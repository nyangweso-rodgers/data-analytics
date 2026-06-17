with
--------------------- Accounts ----------------------------------
accounts_cte as (
        select *
        from (
                SELECT  id,
                customerId,
                accountRef,
                status,
                jsfId,
                --fullDepositDate,
                --accountBalance,
                accountTypeId,
                dispatchDate,
                jsfDate,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.accounts
                ) where rnk = 1
        ),
--------------------- Customers ----------------------------------
customers_cte as (
    SELECT *
    from (
        select id,
        identificationNumber,
        customerTypeId,
        companyRegionId,
        dateOfBirth,
        case
            when gender = 'MALE' then 'Male'
            when gender = 'FEMALE' then 'Female'
            when gender = 'OTHER' then 'Other'
            when gender = '' then null
        else gender end as gender,
        latitude,
        longitude,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.customers
        ) where rnk = 1 
    ),
--------------------- Account Types ----------------------------------
account_types_cte as (
        select * 
        from (
                SELECT id,
                accountType,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.account_types
                ) where rnk = 1
        ),
--------------------- Account Payplans ----------------------------------
account_payplans_cte as (
        select *
        from (
                SELECT id,
                accountId,
                payplanId,
                productQty,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.account_payplans
                ) where rnk = 1
        ),
--------------------- Payplans ----------------------------------
payplans_cte as (
        select *
        from (
                SELECT id,
                name,
                productId,
                depositAmount,
                installmentAmount,
                totalNumberPayments,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.payplans
                ) where rnk = 1
        ),
--------------------- Products  ----------------------------------
products_cte as (
        select *
        from (
                SELECT id,
                product,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.products
                ) where rnk = 1
        ),
--------------------- Company Regions ----------------------------------
company_regions_cte as (
        select distinct id,
        region
        from (
                SELECT id,
                region,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.company_regions
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
    select * 
    from (
        SELECT id,
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
        paymentDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.wallet_installment_payments
        ) where rnk = 1
    ),
--------------------- KYC Requests ----------------------------------
kyc_requests_cte as (
    select *
    FROM (
        SELECT id,
        idNumber,
        nullIf(dob, toDate32('1970-01-01')) as dob,
        gender,
        row_number() OVER (partition by idNumber ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.kyc_requests
        ) where rnk = 1
        --and status = 'SUCCESS'
    ),
--------------------- Premises ----------------------------------
premises_cte AS
(
    SELECT *
    FROM
    (
        SELECT id,
        premise_name,
        customer_id,
        premise_type_id,
        substate_id,
        town,
        row_number() OVER (partition by customer_id ORDER BY updated_at DESC) as rnk 
        from fma.premises
    ) WHERE rnk = 1
),
--------------------- Premise Details ----------------------------------
premise_details_cte AS
    (
        SELECT *
        FROM
        (
            SELECT  premise_id,
            --village,
            subcounty,
            toString(latitude) as latitude,
            toString(longitude) as longitude,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            from fma.premise_details
        ) WHERE rnk = 1
    ),
--------------------- Sub-County ----------------------------------
sub_county_cte AS
    (
        SELECT *
        FROM (
            SELECT  id,
            substate_type,
            substate_name,
            state_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.substates
        ) WHERE rnk = 1 and substate_type =  'Sub County'
    ),
--------------------- States ----------------------------------
states_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            state_name,
            state_type,
            region_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.states
        ) WHERE rnk = 1
    ),
--------------------- County ----------------------------------
county_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            state_type,
            state_name,
            region_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.states
        ) WHERE rnk = 1 and state_type = 'County'
    ),
--------------------- Substates ----------------------------------
subcounty_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            substate_type,
            substate_name,
            state_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            from fma.substates
        ) WHERE rnk = 1
        and substate_type =  'Sub County'
    ),
--------------------- Regions ----------------------------------
regions_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            region_name,
            --country_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.regions
        ) WHERE rnk = 1
    ),
--------- Mashup ----------------------------------
accounts_schedules_mashup_cte as (
        select *
        from (
            select distinct accounts_cte.id as accountId,
            accounts_cte.accountRef as accountRef,
            accounts_cte.customerId as customerId,
            date(accounts_cte.dispatchDate) as dispatchDate,
            accounts_cte.jsfDate as jsfDate,
            account_types_cte.accountType as accountType,
            accounts_cte.status as accountStatus,
            products_cte.product as productName,
            account_payplans_cte.productQty as productQty,
            payplans_cte.depositAmount as depositAmount,
            payplans_cte.installmentAmount as installmentAmount,
            ((payplans_cte.installmentAmount * payplans_cte.totalNumberPayments) + payplans_cte.depositAmount) as totalPayplanAmount,
            payplans_cte.totalNumberPayments as totalNumberPayments,
            installment_schedules_cte.paymentSequence as paymentSequence,
            installment_schedules_cte.id as installmentScheduleid,
            installment_schedules_cte.installmentType as installmentType,
            installment_schedules_cte.expectedDate,
            installment_schedules_cte.expectedAmount as expectedAmount,
            wallet_installment_payments_cte.paymentId as paymentId,
            wallet_installment_payments_cte.ledgerEntryId as ledgerEntryId,
            wallet_installment_payments_cte.paymentDate as paymentDate,
            coalesce(wallet_installment_payments_cte.amountPaid,0) as amountPaid,
            company_regions_cte.region as country,
            states_cte.state_name AS county,
            subcounty_cte.substate_name AS subcounty,
            coalesce(premise_details_cte.latitude, customers_cte.latitude) as latitude,
            coalesce(premise_details_cte.longitude, customers_cte.longitude) as longitude,
            coalesce(kyc_requests_cte.gender, customers_cte.gender) as gender,
            coalesce(kyc_requests_cte.dob, customers_cte.dateOfBirth) as dob
            from accounts_cte 
            left join customers_cte on customers_cte.id = accounts_cte.customerId
            left join account_types_cte on account_types_cte.id = accounts_cte.accountTypeId
            left join account_payplans_cte on account_payplans_cte.accountId = accounts_cte.id
            left join payplans_cte on payplans_cte.id = account_payplans_cte.payplanId
            left join products_cte on products_cte.id = payplans_cte.productId
            left join company_regions_cte on company_regions_cte.id = customers_cte.companyRegionId
            left join installment_schedules_cte on installment_schedules_cte.accountId = accounts_cte.id
            left join wallet_installment_payments_cte on wallet_installment_payments_cte.accountId = installment_schedules_cte.accountId and wallet_installment_payments_cte.instalmentScheduleId = installment_schedules_cte.id
            left join kyc_requests_cte on kyc_requests_cte.idNumber = customers_cte.identificationNumber
            left join premises_cte on premises_cte.customer_id = customers_cte.id
            left join premise_details_cte on premise_details_cte.premise_id = premises_cte.id
            left join subcounty_cte on subcounty_cte.id = premises_cte.substate_id
            left join states_cte on states_cte.id = subcounty_cte.state_id
            left join regions_cte on regions_cte.id = states_cte.region_id
        )
        where productName NOT IN ('Transport / Shipping', 'TSR', 'TSR Uganda', 'Training', 'UG Extra Items', 'Extra Items', 'Installation', 'furrow', 'AfterSale', 'Agronomy', 'Samsung Galaxy A11', 'Kilimo Boost') 
        --and accountType = 'PAYG'
        --and accountStatus not in ('No Deposit', 'Partial Deposit', 'Refunded', 'Pending Installation', 'Full Deposit', 'Rejected', 'On Hold', 'Partial Refunded')
        --and accountStatus in ('Complete', 'Complete - Over Paid', 'Repossession', 'REPOSSESSION', 'Current', 'Arrears', 'Pending Repossession', 'Advance', 'Write Off')
        --and accountStatus in ('Repossession', 'REPOSSESSION')
        --and accountStatus in ('Write Off')
        order by customerId,accountRef, paymentSequence, paymentDate
	),
--------- Mashup ----------------------------------
agg_account_schedules_cte as (
    select distinct accountId,
    customerId,
    accountStatus,
    productName,
    dispatchDate,
    jsfDate,
    expectedDate,
    paymentSequence,
    expectedAmount,
    country
    from accounts_schedules_mashup_cte
    order by customerId,accountRef, paymentSequence
    )
select *
--from accounts_schedules_mashup_cte
from agg_account_schedules_cte
--where accountRef = '6654233-2'
where customerId = '71507'
--where accountId = '50'
limit 1000