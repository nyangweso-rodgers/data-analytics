UPDATE amtdb.employees
SET identificationNumber = CASE id
    WHEN 4409 THEN LTRIM(identificationNumber)
    ELSE identificationNumber
END
WHERE id IN (4409)