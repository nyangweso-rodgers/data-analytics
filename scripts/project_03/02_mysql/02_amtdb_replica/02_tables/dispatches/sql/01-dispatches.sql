with
dispatched_cte as (
	SELECT id, 
	#old_id, 
	installationId, 
	#old_installation_id, 
	dispatchDate, 
	dispatchItems, 
	warehouse, 
	#old_warehouse, 
	dispatcher, 
	`timestamp`, 
	courierDate, 
	dispatchedDeviceId, 
	waybillNumber, 
	paygContractNumber, 
	orderNumber, 
	#createdBy, 
	#old_created_by, 
	createdAt, 
	updatedBy, 
	updatedAt
	FROM amtdb.dispatches
	)
select * from dispatched_cte