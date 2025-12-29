UPDATE amtdb.employees
set identificationNumber = CASE id
	WHEN 4315 THEN RTRIM(identificationNumber)
	ELSE identificationNumber 
END
WHERE id in (4315)