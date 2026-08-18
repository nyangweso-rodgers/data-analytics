WITH
device_cte as (
    SELECT *,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
    FROM sentinel_prod.Device
    ) 
select *
from device_cte
LIMIT 1000