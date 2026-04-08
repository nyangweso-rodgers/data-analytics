UPDATE `sales-service`.leads
SET agentProviderId = CASE leadId

ELSE agentProviderId
END 
WHERE leadId in ()