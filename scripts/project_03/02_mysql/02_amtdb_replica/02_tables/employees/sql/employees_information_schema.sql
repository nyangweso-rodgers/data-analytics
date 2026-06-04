SELECT 
    COLUMN_NAME,
    COLUMN_TYPE,      -- includes length/precision, e.g. varchar(255)
    COLUMN_KEY,       -- e.g. 'PRI' for primary key
    EXTRA,            -- e.g. 'auto_increment'
    COLUMN_DEFAULT,
    IS_NULLABLE
    #CHARACTER_SET_NAME,
    #COLLATION_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'amtdb'
  AND TABLE_NAME = 'employees';