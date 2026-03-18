UPDATE `sales-service`.cds
SET customerId = CASE leadId
    ELSE customerId
END 
WHERE leadId in (
)