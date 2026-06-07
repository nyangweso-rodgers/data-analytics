with 
sms_logs_cte as (
	SELECT id, recipient, payload, provider, response, "createdAt", status, "updatedAt", region, "messageId", "failureReason", sender, app, "cost", currency
	FROM public."SmsLogs"
	)
select --*
count(*)
from sms_logs_cte
limit 1000