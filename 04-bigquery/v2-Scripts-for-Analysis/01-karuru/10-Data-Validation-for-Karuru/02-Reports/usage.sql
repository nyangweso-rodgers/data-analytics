SELECT distinct record_type FROM `kyosk-prod.google_workspace.usage` 
WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) > TIMESTAMP("2024-03-01") LIMIT 1000