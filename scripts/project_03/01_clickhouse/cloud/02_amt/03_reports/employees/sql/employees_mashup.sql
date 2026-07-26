WITH
--------------------- Employees ----------------------------------
employees_cte as (
    select *
    from (
        SELECT  createdAt,
        endDate,
        id, 
        name,
        departmentId,
        --gender,
        email,
        identificationNumber,
        phoneNumber,
        status,
        supervisorId,
        primaryRoleId,
        createdBy,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.employees
    ) where rnk = 1
),
--------------------- View Types ----------------------------------
view_types_cte as (
    select *
    from (
        SELECT id,
        name,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
    FROM amt.view_types
    ) where rnk = 1
    ),
--------------------- Employees - Mashup ----------------------------------
employees_mashup_cte as (
    select distinct 
    employees_cte.identificationNumber as employeeIdentificationNumber,
    nullIf(employees_cte.email, '') as employeeEmail,
    employees_cte.name as employeeName,
    employees_cte.id as employeeId,
    employees_cte.phoneNumber as employeePhoneNumber,
    nullIf(view_types_cte.name, '') as primaryRole,
    employees_cte.status as employeeStatus,
    employees_cte.supervisorId as supervisorId,
    supervisors_cte.name as supervisorName,
    employees_cte.createdBy as employeeCreatedById,
    employees_created_by_cte.name as employeeCreatedByName
    from employees_cte
    left join view_types_cte on view_types_cte.id = employees_cte.primaryRoleId
    left join employees_cte as supervisors_cte on supervisors_cte.id = employees_cte.supervisorId
    left join employees_cte as employees_created_by_cte on employees_created_by_cte.id = employees_cte.createdBy
    ORDER BY employeeName
)
select *
--count(distinct employeeId), count(distinct employeeEmail)
from employees_mashup_cte
--where employeeIdentificationNumber in ('')
--where employeeEmail in ('')
--where employeeName = ''