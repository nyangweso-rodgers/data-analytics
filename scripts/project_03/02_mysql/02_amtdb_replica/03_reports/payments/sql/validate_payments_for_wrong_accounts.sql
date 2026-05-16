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
accountRef_wrong_customer AS (
    SELECT DISTINCT 
        payments_cte.id AS payment_id,
        payments_cte.customerId AS payment_customerId,
        payments_cte.accountRef AS payment_accountRef,
        payments_cte.paymentRef,
        payments_cte.amount,
        payments_cte.timestampMade,
        accounts_cte.customerId AS actual_account_customerId,
        customers_cte.companyRegionId
    FROM payments_cte
    LEFT JOIN customers_cte ON customers_cte.id = payments_cte.customerId
    -- Join to find what customer this accountRef actually belongs to
    INNER JOIN accounts_cte ON accounts_cte.accountRef = payments_cte.accountRef
    WHERE payments_cte.customerId != accounts_cte.customerId  -- Wrong customer!
    ORDER BY timestampMade DESC
)
select *
from accountRef_wrong_customer