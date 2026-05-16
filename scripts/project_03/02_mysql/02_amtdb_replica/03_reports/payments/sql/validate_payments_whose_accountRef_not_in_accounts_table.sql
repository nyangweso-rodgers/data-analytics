WITH 
accounts_cte as (
	SELECT id, customerId, accountRef,
	status
	FROM amtdb.accounts
	),
payments_cte as (
	SELECT id, customerId, accountRef, 
	amount, 
	paymentRef, accountId,
	timestampMade 
	FROM amtdb.payments
	),
customers_cte as (
	SELECT id, 
	companyRegionId
	FROM amtdb.customers
	),
acccountRef_not_in_accounts_table AS (
    SELECT DISTINCT 
        payments_cte.accountRef,
        payments_cte.customerId,
        customers_cte.companyRegionId,
        payments_cte.paymentRef,
        payments_cte.amount,
        payments_cte.timestampMade
    FROM payments_cte
    LEFT JOIN customers_cte ON customers_cte.id = payments_cte.customerId
    LEFT JOIN accounts_cte ON accounts_cte.accountRef = payments_cte.accountRef
    WHERE accounts_cte.accountRef IS NULL  -- accountRef not found in accounts table
    ORDER BY timestampMade DESC
)
select *
from acccountRef_not_in_accounts_table