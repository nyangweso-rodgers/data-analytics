with
employees_cte as (
	SELECT id, 
	#old_id, 
	name, supervisorId, departmentId, 
	#departmentId_old, commissionPayplanId, commissionPayplanId_old, 
	identificationNumber, gender, dob, 
	#email, slack, 
	phoneNumber, 
	#mobileMoneyPhoneNumber, preferLanguage, recommendationLetter, employeePic, employeeIdPic, employeeContract, countryId, countryId_old, 
	status, isCustomer, roleId, roleId_old, endDate, createdBy, 
	#created_by_old, 
	createdAt, updatedBy, 
	#updated_by_old, 
	updatedAt, primaryRoleId, 
	#salesForceAgentId, contractType, authServiceId, isUssdEnabled, isSalesAppEnabled, 
	salesTrainingCompletionDate
	#referedBy
	FROM amtdb.employees
	)
select *
#min(endDate)
from employees_cte
#where date(endDate) < '1970-01-01'
where id in ('707',
'1004',
'2499',
'2810')