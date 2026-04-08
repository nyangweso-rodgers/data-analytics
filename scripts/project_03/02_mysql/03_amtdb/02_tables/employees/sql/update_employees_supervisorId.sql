UPDATE amtdb.employees
SET supervisorId = CASE id
   
    ELSE supervisorId
END
WHERE id IN ()