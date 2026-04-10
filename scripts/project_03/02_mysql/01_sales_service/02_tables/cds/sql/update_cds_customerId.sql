-- update using leadId
UPDATE `sales-service`.cds
SET customerId = CASE leadId
    ELSE customerId
END 
WHERE leadId in (
)

-- update using accountId
UPDATE `sales-service`.cds
SET customerId = CASE accountId
    ELSE customerId
END 
WHERE accountId in ()