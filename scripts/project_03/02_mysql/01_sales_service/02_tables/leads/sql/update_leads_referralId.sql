UPDATE `sales-service`.leads
SET referralId = CASE leadId
WHEN '00Q8d000004TQhUEAW' THEN '254111696203'
ELSE referralId
END 
WHERE leadId in ('00Q8d000004TQhUEAW')