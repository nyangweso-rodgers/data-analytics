WITH
users_cte as (
    SELECT * 
    FROM ccs.users
    ) 
select 
max(_synced_at)
from users_cte
LIMIT 31 