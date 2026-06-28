with
employees_cte as (
	SELECT id, 
	#old_id, 
	name, supervisorId, departmentId, 
	#departmentId_old, commissionPayplanId, commissionPayplanId_old, 
	identificationNumber, gender, dob, email, 
	#slack, 
	phoneNumber, 
	#mobileMoneyPhoneNumber, preferLanguage, recommendationLetter, employeePic, employeeIdPic, employeeContract, countryId, countryId_old, 
	status, 
	#isCustomer, roleId, roleId_old, 
	endDate, 
	createdBy, 
	-- created_by_old, 
	createdAt, 
	updatedBy, 
	#updated_by_old, 
	updatedAt, 
	primaryRoleId,
	#salesForceAgentId, contractType, authServiceId, isUssdEnabled, isSalesAppEnabled
	salesTrainingCompletionDate 
	FROM amtdb.employees
	),
view_types_cte as (
	SELECT id, 
	name
	#createdBy, createdAt, updatedBy, updatedAt
	FROM amtdb.view_types
	order by name
	),
departments_cte as (
	SELECT id, 
	#old_id, companyRegionId, 
	name 
	#hod, createdAt, updatedAt
	FROM amtdb.departments
	),
employee_regions_cte as (
	SELECT id, companyRegionId, employeeId
	#createdAt, updatedAt
	FROM amtdb.employee_regions
	),
company_regions_cte as (
	SELECT id, 
	region
	#companyName createdAt, createdBy, updatedAt, updatedBy, defaultCurrencyId
	FROM amtdb.company_regions
	),
employees_mashup_cte as (
	select distinct employees_cte.id as employeeId,
	GROUP_CONCAT(DISTINCT employee_company_regions_cte.region SEPARATOR ' / ') AS employeeRegions,
	employees_cte.status as employeeStatus,
	employees_cte.identificationNumber as employeeIdentificationNumber,
	#CHAR_LENGTH(employees_cte.identificationNumber) as employeeIdentificationNumberCharCount, -- data validation
	employees_cte.phoneNumber as employeePhoneNumber,
	employees_cte.name as employeeName,
	employees_cte.gender as employeeGender,
	employees_cte.primaryRoleId as primaryRoleId,
	view_types_cte.name as employeePrimaryRole,
    departments_cte.name as employeeDepartment,
    -- employees_cte.dob as employeeDob,
    date(supervisor_cte.createdAt) as supervisorCreatedAt,
    date(supervisor_cte.endDate) as supervisorEndDate,
    employees_cte.supervisorId as supervisorId,
    supervisor_cte.status as supervisorStatus,
    supervisor_cte.name as supervisorName,
    supervisor_role.name as supervisorPrimaryRole,
    supervisor_department_cte.name as supervisorDepartment,
    -- supervisor_cte.dob as supervisorDob
    employees_cte.createdBy as employeeCreatedBy,
	employee_created_by_cte.name as employeeCreatedByName,
	employees_cte.updatedBy as employeeUpdatedBy,
	employee_updated_by_cte.name as employeeUpdatedByName,
    employees_cte.salesTrainingCompletionDate as salesTrainingCompletionDate,
    date(employees_cte.endDate) as employeeEndDate,
    employees_cte.createdAt as employeeCreatedAt,
	employees_cte.updatedAt as employeeUpdatedAt
	from employees_cte
	left join employees_cte as employee_created_by_cte on employee_created_by_cte.id = employees_cte.createdBy 
	left join employees_cte as employee_updated_by_cte on employee_updated_by_cte.id = employees_cte.updatedBy
	left join view_types_cte on view_types_cte.id = employees_cte.primaryRoleId 
    left join departments_cte on departments_cte.id = employees_cte.departmentId
    left join employee_regions_cte on employee_regions_cte.employeeId = employees_cte.id
    left join company_regions_cte as employee_company_regions_cte on employee_company_regions_cte.id =  employee_regions_cte.companyRegionId
    left join employees_cte as supervisor_cte on supervisor_cte.id = employees_cte.supervisorId
    left join view_types_cte as supervisor_role on supervisor_role.id = supervisor_cte.primaryRoleId
    left join departments_cte as supervisor_department_cte on supervisor_department_cte.id = supervisor_cte.departmentId
    GROUP BY 1,3,4,5,6,7,8,9,10,11,12,13,14,15,16
	),
sales_agents_cte as (
	select distinct employeeIdentificationNumber,
	employeeId,
	employeePrimaryRole,
	salesTrainingCompletionDate
	from employees_mashup_cte
	ORDER BY salesTrainingCompletionDate DESC
	),
supervisors_cte as (
	select distinct employeeRegions,
	employeeCreatedAt,
	employeeUpdatedAt,
	employeeEndDate,
	employeeStatus,
	employeeId,
	#employeeIdentificationNumber,
	employeePhoneNumber,
	employeeName,
	employeeGender,
	employeePrimaryRole,
	employeeDepartment,
	supervisorName,
	employeeCreatedBy,
	employeeUpdatedBy,
	employeeCreatedByName,
	employeeUpdatedByName
	from employees_mashup_cte
	where employeeRegions = 'kenya'
	and employeePrimaryRole in ('Sales Team Lead', 'Telesales Team Lead')
	)
select #*
#distinct employeePrimaryRole
distinct employeePrimaryRole, count(distinct employeeId)
#count(*)
-- distinct employeeIdentificationNumberCharCount, count(distinct employeeId)
from employees_mashup_cte
#from supervisors_cte
#from sales_agents_cte
#where employeeRegions = 'kenya'
where employeeRegions = 'uganda'
#and employeeStatus = 'Active'
#and employeeIdentificationNumberCharCount is NULL
#where employee_primary_role_name = 'POP Agents'
#where identificationNumber=21355245
#where employeeName like "Rodgers Nyangweso%"
group by 1 order by 2 desc
#where employeePrimaryRole = 'POP Agent'
#where employeeIdentificationNumber in ()
#where employeeId in ('8503', '2631')
#where employeeIdentificationNumber = '27829914'
#where employeeIdentificationNumber = '29377220'
#where employeeIdentificationNumber = '11564500'
#where employeeIdentificationNumber = '37121359'