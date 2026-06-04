with
system_logs_cte as (
	SELECT id, 
	module, 
	oldPayload, 
	newPayload, 
	#note, 
	#createdAt, 
	#createdBy, 
	updatedAt
	#updatedBy
	FROM amtdb.system_logs
	),
employee_changes_cte AS (
    SELECT id,
        updatedAt,
        CASE
            WHEN JSON_VALID(oldPayload)
            THEN oldPayload->>'$.id'
            ELSE NULL
        END as employeeId,
        CASE
            WHEN JSON_VALID(oldPayload)
            THEN oldPayload->>'$.identificationNumber'
            ELSE NULL
        END as identificationNumber,
        CASE
            WHEN JSON_VALID(oldPayload)
            THEN oldPayload->>'$.supervisorId'
            ELSE NULL
        END as oldSupervisorId,
        CASE
            WHEN JSON_VALID(newPayload)
            THEN newPayload->>'$.supervisorId'
            ELSE NULL
        END as newSupervisorId,
        -- Check if supervisorId key exists in newPayload
        CASE
            WHEN JSON_VALID(newPayload)
            THEN JSON_CONTAINS_PATH(newPayload, 'one', '$.supervisorId')
            ELSE 0
        END as supervisorId_in_new
    FROM system_logs_cte
    WHERE module = 'employee'
        AND COALESCE(oldPayload, '') != ''
        AND COALESCE(newPayload, '') != ''
),
employee_supervisor_changes_cte as (
	SELECT 
    employeeId,
    identificationNumber,
    oldSupervisorId,
    newSupervisorId,
    supervisorId_in_new,
    updatedAt,
    CASE 
        WHEN supervisorId_in_new = 0 THEN 'Field not changed'
        WHEN supervisorId_in_new = 1 AND newSupervisorId IS NULL THEN 'Supervisor removed'
        WHEN supervisorId_in_new = 1 AND newSupervisorId IS NOT NULL THEN 'Supervisor changed'
    END as change_type
FROM employee_changes_cte
WHERE supervisorId_in_new = 1  -- Only rows where supervisorId was actually updated
    AND (oldSupervisorId IS NULL OR newSupervisorId IS NULL OR oldSupervisorId != newSupervisorId)
ORDER BY employeeId, updatedAt DESC
)
select *
from employee_changes_cte
#from employee_supervisor_changes_cte 
where identificationNumber = '21729904'