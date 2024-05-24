------------ Karuru --------------
----------- Stock Balance -----------
SELECT max(bq_upload_time) 
FROM `kyosk-prod.karuru_reports.stock_balance` 
WHERE date(updated_on) >= '2024-01-01'