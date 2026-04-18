UPDATE `sales-service`.leads
SET referralId = CASE leadId
WHEN '' THEN ''
ELSE referralId
END 
WHERE leadId in ()