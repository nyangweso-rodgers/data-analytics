# Stored Procedure

# View Stored Procedure Logic

- Use:
  ```sql
    show create procedure <name>
  ```
- This will show you the full SQL definition, including:
  - INSERT logic
  - UPDATE / DELETE logic
  - Any transformations
  - Joins
  - Intermediate temp tables
  - Business rules

# View procedure metadata

- Use:
  ```sql
    select *
    from information_schema.ROUTINES
    WHERE ROUTINE_NAME = '<name>'
    AND ROUTINE_TYPE = 'PROCEDURE'
  ```
