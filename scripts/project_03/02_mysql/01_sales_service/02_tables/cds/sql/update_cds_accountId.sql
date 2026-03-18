UPDATE `sales-service`.cds
SET accountId = CASE leadId

    ELSE accountId
END 
WHERE leadId in (
)