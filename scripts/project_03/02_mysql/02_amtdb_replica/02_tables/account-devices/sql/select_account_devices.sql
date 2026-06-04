with
account_devices_cte as (
	SELECT id, 
	accountId, 
	jsfId, 
	deviceId, 
	isDecommissioned, 
	deviceStatus, 
	distributorId, 
	createdAt, 
	createdBy, 
	updatedAt, 
	updatedBy, 
	isValidated, 
	matchLikelihood
	FROM amtdb.account_devices
	)
SELECT 
count(*)
from account_devices_cte
where accountId is null
#order by updatedAt  desc
#where deviceId = '861045087674712'