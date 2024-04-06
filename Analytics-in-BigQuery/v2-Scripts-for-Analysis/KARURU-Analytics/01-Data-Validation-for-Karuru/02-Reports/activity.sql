SELECT distinct 
time_usec,
TIMESTAMP_MICROS(time_usec),
--cast(time_usec as timestamp)
--SAFE_CAST(SAFE_CAST(time_usec as string) as timestamp),
--PARSE_DATE('%d%m%Y', cast(time_usec as string)),
email,
event_type,
event_name, 
unique_identifier, 
--group_id,
event_id,
data_studio.owner_email,
data_studio.asset_id,
data_studio.asset_type,
data_studio.asset_name,
data_studio.target_domain,
data_studio.data_export_type,
data_studio.connector_type,
--data_studio.current_value,
--data_studio.target_domain,
--data_studio.target_user_email,
data_studio.visibility,
--data_studio.distribution_content_id,
--data_studio.distribution_content_name,
--data_studio.distribution_content_owner_email,
--data_studio.distribution_content_type
FROM `kyosk-prod.google_workspace.activity` WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) > TIMESTAMP("2024-03-01") 
--and data_studio.asset_type = 'REPORT'
and data_studio.asset_id is not null
--and data_studio.asset_id = '34cc9626-dc88-4cfe-852a-7f5060110c1c'
order by email