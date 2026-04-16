with
devices_cte as (
	SELECT id, "isTagged", "lastTimeConnected", "iot", "statusDesired", "statusReported", "methodOfCreation", "endpointId", latitude, longitude, "projectId", "productId", "companyId", meta, "createdAt", "createdBy", "updatedAt", "updatedBy", "batchId", "deviceId", error, "lockStatus", "lastIotCommandAt", "lockHoursDesired", "lockHoursReported", "LVD24VCount", "clientName", "dryRunCount", "dryRunDetectedTimes", "firmwareVersion", "phoneNumber", "retiredDevice", variant, "migrationStatus", "customerId", "isDecommissioned", "lastReportTime"
	FROM public."Device"
	)
select distinct iot
from devices_cte