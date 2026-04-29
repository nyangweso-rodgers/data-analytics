# List all stored procedures in a database:
SHOW PROCEDURE STATUS WHERE Db = 'amtdb';

# See all procedures you have access to:
SHOW PROCEDURE STATUS;

# View the stored procedure code:
SHOW CREATE PROCEDURE procedure_name;

# Query the information schema:
SELECT * FROM information_schema.ROUTINES 
WHERE ROUTINE_TYPE = 'PROCEDURE' 
AND ROUTINE_SCHEMA = 'your_database_name';

# For a specific procedure:
SELECT * FROM information_schema.ROUTINES 
WHERE ROUTINE_TYPE = 'PROCEDURE' 
AND ROUTINE_NAME = 'procedure_name'
AND ROUTINE_SCHEMA = 'your_database_name';

# Get just the procedure definition:
SELECT ROUTINE_DEFINITION 
FROM information_schema.ROUTINES 
WHERE ROUTINE_NAME = 'procedure_name' 
AND ROUTINE_SCHEMA = 'your_database_name';