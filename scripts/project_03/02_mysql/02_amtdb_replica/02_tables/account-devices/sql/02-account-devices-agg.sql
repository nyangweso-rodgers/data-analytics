SELECT 
distinct deviceStatus,
count(distinct id)
FROM amtdb.account_devices
group by 1