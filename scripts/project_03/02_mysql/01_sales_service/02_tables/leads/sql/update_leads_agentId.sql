UPDATE `sales-service`.leads
SET agentId = CASE leadId

ELSE agentId
END 
WHERE leadId in ()