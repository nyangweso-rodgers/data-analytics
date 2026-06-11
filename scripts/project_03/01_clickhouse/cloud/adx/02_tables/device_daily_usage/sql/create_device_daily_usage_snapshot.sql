CREATE TABLE adx.device_daily_usage_v1
ENGINE = MergeTree()
ORDER BY (deviceId)
AS SELECT * FROM adx.device_daily_usage;