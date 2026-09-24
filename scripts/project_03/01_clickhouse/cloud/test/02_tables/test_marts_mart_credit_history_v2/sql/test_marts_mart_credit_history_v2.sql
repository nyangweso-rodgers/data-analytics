WITH
--------------------- test - test_marts_mart_credit_history_v2 ----------------------------------
test_marts_mart_credit_history_v2_cte as (
    SELECT * 
    FROM test.test_marts_mart_credit_history_v2
    ORDER BY accountId, month
    ) 
select *
from test_marts_mart_credit_history_v2_cte
--WHERE accountId = '72118' # first paymentDate < jsf_date
where accountRef = '26134441-02'
--where accountRef = '9660270'
--where month = '2026-03-01'
LIMIT 1000