#
UPDATE `sales-service`.leads
SET leadSourceId = 107
WHERE leadId in ()


# Update leadSourceId for specific leadIds in the leads table of the sales-service database.
UPDATE `sales-service`.leads
SET leadSourceId = CASE leadId

ELSE leadSourceId
END 
WHERE leadId in ()