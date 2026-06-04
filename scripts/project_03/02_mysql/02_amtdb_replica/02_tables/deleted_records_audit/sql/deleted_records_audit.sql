with
deleted_records_audit_cte as (
	SELECT id, 
	tableName, 
	recordId, 
	deletedData, 
	deletedAt
	FROM amtdb.deleted_records_audit
	)
SELECT * from deleted_records_audit_cte