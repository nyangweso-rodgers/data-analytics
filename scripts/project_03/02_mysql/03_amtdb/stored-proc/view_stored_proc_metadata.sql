select *
    from information_schema.ROUTINES
    WHERE ROUTINE_NAME = '<name>'
    AND ROUTINE_TYPE = 'PROCEDURE'