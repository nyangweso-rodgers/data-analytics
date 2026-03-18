with
employees_cte as (
	SELECT id, 
	#old_id, name, supervisorId, departmentId, departmentId_old, commissionPayplanId, commissionPayplanId_old, 
	identificationNumber, 
	#gender, dob, email, slack, 
	phoneNumber, mobileMoneyPhoneNumber, 
	#preferLanguage, recommendationLetter, employeePic, employeeIdPic, employeeContract, countryId, countryId_old, status, isCustomer, 
	roleId, roleId_old, endDate, createdBy, created_by_old, createdAt, updatedBy 
	#updated_by_old, updatedAt, primaryRoleId, 
	#salesForceAgentId, contractType, authServiceId, isUssdEnabled, isSalesAppEnabled
	FROM amtdb.employees
	),
validate_trailing_whitespaces_cte as (
	select #count(*)
	id,
	identificationNumber,
	#HEX(identificationNumber) as hex_value,
	LENGTH(identificationNumber) as current_length,
	RTRIM(identificationNumber) as trimmed_value, 
    LENGTH(RTRIM(identificationNumber)) as new_length
	from employees_cte
	where identificationNumber like " %"
	),
validate_leading_whitespaces_cte as (
	SELECT id, 
       identificationNumber as current_value,
       LTRIM(identificationNumber) as new_value,
       CHAR_LENGTH(identificationNumber) as current_length,
       CHAR_LENGTH(LTRIM(identificationNumber)) as new_length
	FROM amtdb.employees
	WHERE identificationNumber LIKE " %"
	)
select * 
#from employees_cte
#from validate_trailing_whitespaces_cte
from validate_leading_whitespaces_cte