-- Foreign Key Dependencies Order: Try truncating in a specific order, or use a transaction:
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
TRUNCATE TABLE ... RESTART IDENTITY CASCADE;
COMMIT;