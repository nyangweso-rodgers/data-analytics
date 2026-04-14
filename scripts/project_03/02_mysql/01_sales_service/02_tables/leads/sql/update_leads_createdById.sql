UPDATE `sales-service`.leads
SET createdById = CASE leadId

ELSE createdById
END 
WHERE leadId in ()