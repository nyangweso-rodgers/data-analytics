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
	#updatedBy, updated_by_old, 
	updatedAt, 
	primaryRoleId
	#salesForceAgentId, contractType, authServiceId, isUssdEnabled, isSalesAppEnabled
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
	select distinct 
	GROUP_CONCAT(DISTINCT employee_company_regions_cte.region SEPARATOR ' / ') AS employeeRegions,
	employees_cte.createdBy as employeeCreatedBy,
	employee_created_by_cte.name as employeeCreatedByName,
	employees_cte.createdAt as employeeCreatedAt,
	employees_cte.updatedAt as employeeUpdatedAt,
	date(employees_cte.endDate) as employeeEndDate,
	employees_cte.status as employeeStatus,
	employees_cte.id as employeeId,
	employees_cte.identificationNumber as employeeIdentificationNumber,
	CHAR_LENGTH(employees_cte.identificationNumber) as employeeIdentificationNumberCharCount, -- data validation
	employees_cte.phoneNumber as employeePhoneNumber,
	employees_cte.name as employeeName,
	view_types_cte.name as employeePrimaryRole,
    departments_cte.name as employeeDepartment,
    -- employees_cte.dob as employeeDob,
    date(supervisor_cte.createdAt) as supervisorCreatedAt,
    date(supervisor_cte.endDate) as supervisorEndDate,
    employees_cte.supervisorId as supervisorId,
    supervisor_cte.status as supervisorStatus,
    supervisor_cte.name as supervisorName,
    supervisor_role.name as supervisorPrimaryRole,
    supervisor_department_cte.name as supervisorDepartment
    -- supervisor_cte.dob as supervisorDob
	from employees_cte
	left join employees_cte as employee_created_by_cte on employee_created_by_cte.id = employees_cte.createdBy 
	left join view_types_cte on view_types_cte.id = employees_cte.primaryRoleId 
    left join departments_cte on departments_cte.id = employees_cte.departmentId
    left join employee_regions_cte on employee_regions_cte.employeeId = employees_cte.id
    left join company_regions_cte as employee_company_regions_cte on employee_company_regions_cte.id =  employee_regions_cte.companyRegionId
    left join employees_cte as supervisor_cte on supervisor_cte.id = employees_cte.supervisorId
    left join view_types_cte as supervisor_role on supervisor_role.id = supervisor_cte.primaryRoleId
    left join departments_cte as supervisor_department_cte on supervisor_department_cte.id = supervisor_cte.departmentId
    GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15
	),
sales_team_leads_cte as (
	select *
	from employees_mashup_cte
	#where employeeRegions = 'kenya'
	#and employeePrimaryRole = 'Sales Team Lead'
	#where employeeName like '%Katana%'
	-- where employeeId = '8162'
	-- where employeeIdentificationNumber like '21729904'
	/*where employeeIdentificationNumber in ('29268229',
'21729904',
'32051794',
'29354687',
'22201549',
'30071961',
'40847277',
'33953586',
'32156029',
'34874575',
'34934833',
'35702063',
'27385755',
'34021600',
'30706192',
'26259348',
'31013369',
'28749981',
'29041864',
'25765705',
'34290881',
'27810178',
'28485473',
'29179064',
'25087568',
'20681038',
'32509087',
'27008577',
'36022578',
'33051187',
'32143414',
'32731344',
'27698474',
'29503203',
'14705546',
'28707960',
'27187342',
'28775428',
'25092968')*/
	/*and employeeName in ('Ronald Makanga Were', 'Martin Katana',
'Ali Changu',
#'Conrad Ong'amo',
'Godwin Wafula',
'Saiblonn Sakau',
'Gabriel Mogoi',
'Paul Obonyo',
'Winstone Omanyala Ekiring',
'Fredrick Nkarabali',
'JUSTINE KITUM',
'Steven Angina',
'Mariam Maghema',
'John Mwaura Kagia',
'Kelvin Ndungu',
'Lydia Nekesa Waswa',
'Bryson Maganga',
'Gideon Njogah',
'Brian Ochieng',
'Victor Kirui',
'Kennedy Maina',
'Maryam Lugogo',
'Elizabeth Mukiri',
'Dennis Koech',
'Isaac Maina',
'David Therebe',
'Islam Fadhil',
'Hellen Gichia',
'Betty Laboso',
'Benjamin Wairimu',
'Victor Shikhule',
'Evans Musehenga',
'Michael Kofa',
'Peter Mwenda',
'James Gumba',
'Mildred Tonje',
'Victor Owuor',
#'Nickson Oteng'o',
'Brian Onyango')*/
	# confirmed - exists in the db
	#where employeeName in ('Ronald Makanga Were')
	order by employeeName 
	)
select   *
#count(*)
-- distinct employeeIdentificationNumberCharCount, count(distinct employeeId)
from employees_mashup_cte
-- from sales_team_leads_cte
where employeeRegions = 'kenya'
#and employeeStatus = 'Active'
#and employeeIdentificationNumberCharCount is NULL
#where employee_primary_role_name = 'POP Agents'
#where identificationNumber=21355245
#where employeeName like "Rodgers Nyangweso%"
-- group by 1 order by 2 desc
-- where employeeIdentificationNumber in ()
and employeeName = 'Samuel Inchwara'