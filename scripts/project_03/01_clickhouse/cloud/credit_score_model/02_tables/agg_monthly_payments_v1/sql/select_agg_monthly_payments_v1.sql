WITH
agg_monthly_payments_v1_cte as (
    SELECT customerCreatedDate,
    country,
    customerId,
    customerType,
    paymentMonth,
    totalPaymentCount,
    totalPaymentAmount
    FROM credit_score_model.agg_monthly_payments_v1
    ) 
select --*
count(*), count(distinct customerId), max(customerCreatedDate)
from agg_monthly_payments_v1_cte
LIMIT 31