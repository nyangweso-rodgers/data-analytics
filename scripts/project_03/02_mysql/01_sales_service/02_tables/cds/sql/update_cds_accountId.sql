UPDATE `sales-service`.cds
SET accountId = CASE customerId
    ELSE accountId
END 
WHERE customerId in (
'74535'
)


-- update `sales-service`.cds