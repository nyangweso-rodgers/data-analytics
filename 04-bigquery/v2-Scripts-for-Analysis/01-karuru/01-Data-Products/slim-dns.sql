----------- Slim Delivery Notes -----------

SELECT 
distinct status, count(distinct id),
max(created_at) as max_created_at, max(delivery_date) as max_delivery_date, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time,
min(created_at) as min_created_at, min(delivery_date) as min_delivery_date, min(updated_at) as min_updated_at, min(bq_upload_time) as min_bq_upload_time
FROM `kyosk-prod.karuru_reports.slim_delivery_notes` WHERE TIMESTAMP_TRUNC(created_at, DAY) > TIMESTAMP("2021-01-01")
group by 1