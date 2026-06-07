with
--------------------- ADX - Device Daily Usage ----------------------------------
device_daily_usage_cte as (
    SELECT distinct deviceId,
    timestamp,
    energyConsumptionKwh,
    timeIntervalMinutes,
    Source,
    FwVer,
    Variant
    FROM adx.device_daily_usage
    ),
--------------------- Validate Duplicates ----------------------------------
validate_duplicate_cte as (
    select distinct timestamp,
    deviceId,
    count(*) as record_count
    from device_daily_usage_cte
    GROUP BY 1,2
    HAVING record_count > 1
)
select *
--count(*)
--min(timestamp) as min_timestamp, max(timestamp) as max_timestamp, count(*), count( distinct deviceId)
--min(energyConsumptionKwh)
from device_daily_usage_cte
--from validate_duplicate_cte
--where timestamp = '2026-04-27'
--where deviceId = '868328054241050'
--where timeIntervalMinutes = 0
where energyConsumptionKwh = 0
--ORDER BY deviceId, timestamp
ORDER BY timestamp desc
LIMIT 1000;