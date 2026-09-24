with 
agg_device_telemetry_v1_cte as (
	SELECT country, 
	device_timestamp_month, 
	customer_id, 
	device_id_count, 
	account_id_count, 
	days_with_data, 
	total_time_interval_mins, 
	avg_time_interval_mins, 
	avg_energy_consumption_kwh, 
	total_energy_consumption_kwh, 
	total_telemetry_record_count, 
	sync_timestamp
	FROM data_science.agg_device_telemetry_v1
	)
select --*
count(*), max(device_timestamp_month), max(sync_timestamp)
from agg_device_telemetry_v1_cte