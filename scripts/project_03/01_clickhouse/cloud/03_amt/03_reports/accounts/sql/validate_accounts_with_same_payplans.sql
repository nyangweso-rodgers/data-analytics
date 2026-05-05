WITH
--------------------- Accounts ----------------------------------
accounts_cte as (
        select *
        from (
                SELECT distinct id,
                customerId,
                accountRef,
                status,
                salesAgents,
                accountBalance,
                accountTypeId,
                jsfId,
                createdAt,
                fullDepositDate,
                dispatchDate,
                jsfDate,
                --updatedAt,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.accounts
                ) where rnk = 1
        ),
--------------------- Sales ----------------------------------
sales_cte as (
        select distinct account_id,
        sale_date
        from (
                select account_id,
                sale_date,
                row_number() OVER (partition by account_id ORDER BY updatedAt DESC) as rnk 
                from amt.sales
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
                SELECT distinct id,
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
                totalCash,
                totalCashIncVAT,
                totalPaygPriceExVat,
                totalPaygPriceInclVat,
                depositAmount,
                installmentAmount,
                totalNumberPayments,
                paygIntrest,
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
                isRefurb,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.products
                ) where rnk = 1
        ),
--------------------- Customers ----------------------------------
customers_cte as (
    SELECT *
    from (
        select distinct id,
        identificationNumber,
        companyRegionId,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.customers
        ) where rnk = 1 
    ),
--------------------- Company Regions ----------------------------------
company_regions_cte as (
        select *
        from (
                SELECT id,
                region,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.company_regions
                ) where rnk = 1
        ),
--------------------- BOQ ----------------------------------
boq_cte as (
    select *
    from (
        SELECT id,
        accountId,
        boqStatus,
        boqDate,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
        FROM amt.boq
        where boqStatus = 'BILLED'
    ) where rnk = 1
    ),
--------------------- Accounts Mashup ----------------------------------
accounts_mashup_cte as (
        select *
        from (
                select distinct accounts_cte.id as accountId,
                accounts_cte.accountRef,
                customerId,
                company_regions_cte.region as country,
                --customers_cte.identificationNumber as identificationNumber,
                sales_cte.sale_date,
                status,
                accountBalance,
                account_types_cte.accountType as accountType,
                accounts_cte.status as accountStatus,
                products_cte.product as productName,
                account_payplans_cte.productQty as productQty,
                payplans_cte.name as payplanName,
                depositAmount,
                payplans_cte.installmentAmount as installmentAmount,
                payplans_cte.totalNumberPayments as totalNumberPayments, 
                ((payplans_cte.installmentAmount * payplans_cte.totalNumberPayments) + payplans_cte.depositAmount) as totalPayplanAmount,
                accounts_cte.salesAgents,
                boqStatus,
                boqDate,
                jsfId,
                accounts_cte.createdAt as accountCreatedAt,
                accounts_cte.fullDepositDate,
                dispatchDate,
                jsfDate
                from accounts_cte
                left join sales_cte on sales_cte.account_id = accounts_cte.id
                left join customers_cte on customers_cte.id = accounts_cte.customerId
                left join company_regions_cte on company_regions_cte.id = customers_cte.companyRegionId
                left join account_types_cte on account_types_cte.id = accounts_cte.accountTypeId
                left join account_payplans_cte on account_payplans_cte.accountId = accounts_cte.id
                left join payplans_cte on payplans_cte.id = account_payplans_cte.payplanId
                left join products_cte on products_cte.id = payplans_cte.productId
                left join boq_cte on boq_cte.accountId = accounts_cte.id
                )
                where productName NOT IN ('Transport / Shipping', 'TSR', 'TSR Uganda', 'Training', 'UG Extra Items', 'Extra Items', 'Installation', 'furrow', 'AfterSale', 'Agronomy', 'Samsung Galaxy A11', 'Kilimo Boost') 
                and accountType = 'PAYG'
        order by customerId
        ),
---------------------  ----------------------------------
customers_with_multiple_accounts_cte as (
        select distinct customerId,
        count(distinct accountId) as account_id_count
        from accounts_mashup_cte
        group by 1
        ),
--------------------- Customers with Same Payplan Across Accounts ----------------------------------
customers_with_same_payplan_cte as (
        select customerId,
               payplanName,
               count(distinct accountId) as accounts_with_same_payplan
        from accounts_mashup_cte
        where customerId in (select customerId from customers_with_multiple_accounts_cte where account_id_count > 1)
        group by customerId, payplanName
        having count(distinct accountId) > 1
        )
-- Final query to get the details
select distinct 
       a.customerId,
       country,
       c.payplanName,
       c.accounts_with_same_payplan,
       m.account_id_count as total_accounts,
       a.accountId,
       a.accountRef,
       a.accountStatus,
       a.productName,
       a.salesAgents,
       boqStatus,
       date(boqDate),
       jsfId,
       date(accountCreatedAt),
       fullDepositDate,
       date(dispatchDate),
       jsfDate
from customers_with_same_payplan_cte c
join accounts_mashup_cte a on a.customerId = c.customerId and a.payplanName = c.payplanName
join customers_with_multiple_accounts_cte m on m.customerId = c.customerId
order by a.customerId, c.payplanName, a.accountId;