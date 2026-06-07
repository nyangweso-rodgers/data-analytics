WITH
--------------------- Accounts ----------------------------------
accounts_cte as (
        select *
        from (
                SELECT distinct id,
                createdAt,
                --updatedAt,
                customerId,
                accountRef,
                status,
                fullDepositDate,
                accountBalance,
                accountTypeId,
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
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.customers
        ) where rnk = 1 
    ),
--------------------- Accounts Mashup ----------------------------------
accounts_mashup_cte as (
        select *
        from (
                select distinct accounts_cte.id as accountId,
                accounts_cte.accountRef,
                customers_cte.identificationNumber as identificationNumber,
                sales_cte.sale_date,
                status,
                accountBalance,
                account_types_cte.accountType as accountType,
                accounts_cte.status as accountStatus,
                products_cte.product as productName,
                account_payplans_cte.productQty as productQty,
                depositAmount,
                payplans_cte.installmentAmount as installmentAmount,
                payplans_cte.totalNumberPayments as totalNumberPayments, 
                ((payplans_cte.installmentAmount * payplans_cte.totalNumberPayments) + payplans_cte.depositAmount) as totalPayplanAmount
                from accounts_cte
                left join sales_cte on sales_cte.account_id = accounts_cte.id
                left join customers_cte on customers_cte.id = accounts_cte.customerId
                left join account_types_cte on account_types_cte.id = accounts_cte.accountTypeId
                left join account_payplans_cte on account_payplans_cte.accountId = accounts_cte.id
                left join payplans_cte on payplans_cte.id = account_payplans_cte.payplanId
                left join products_cte on products_cte.id = payplans_cte.productId
                )
        where productName = 'Kilimo Boost' 
        )
select *
--count(*), count(distinct accountId) as account_id_count
from accounts_mashup_cte
--where accountId in ()
--where accountRef =''
--where identificationNumber = ''
--limit 100