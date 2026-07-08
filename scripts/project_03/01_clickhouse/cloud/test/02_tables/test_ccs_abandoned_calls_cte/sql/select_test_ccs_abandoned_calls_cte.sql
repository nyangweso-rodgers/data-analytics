WITH
test_ccs_abandoned_calls_cte as (
    SELECT * 
    FROM test.test_ccs_abandoned_calls
    ) 
select *
from test_ccs_abandoned_calls_cte
LIMIT 31 OFFSET 0;