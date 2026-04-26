with
device_daily_usage_cte as (
    SELECT * 
    FROM adx.device_daily_usage
    )
select *
from device_daily_usage_cte
LIMIT 31 OFFSET 0;