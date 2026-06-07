WITH
--------------------- Account Devices ----------------------------------
account_devices_cte as (
    select *
    from (
        SELECT id,
    accountId,
    deviceId,
    deviceStatus,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
    FROM amt.account_devices
    ) where rnk = 1
    ) 
select *
--max(sync_at)
from account_devices_cte
where deviceId = '868613066729519'
LIMIT 31