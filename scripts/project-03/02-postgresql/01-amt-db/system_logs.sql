SELECT 
	id, 
	module, # 11 modules
	oldPayload, 
	newPayload, 
	note, 
	createdAt, 
	createdBy, 
	updatedAt, 
	updatedBy
FROM amtdb.system_logs;