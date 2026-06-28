with
employees_cte as (
SELECT distinct 
#id, old_id, name, supervisorId, departmentId, departmentId_old, commissionPayplanId, commissionPayplanId_old, identificationNumber, gender, dob, email, 
#slack, phoneNumber, mobileMoneyPhoneNumber, preferLanguage, recommendationLetter, employeePic, employeeIdPic, employeeContract, 
#countryId, countryId_old, status, isCustomer, roleId, roleId_old, 
endDate
#createdBy, created_by_old, createdAt, updatedBy, updated_by_old, updatedAt, primaryRoleId, salesForceAgentId, contractType, authServiceId, isUssdEnabled, isSalesAppEnabled, salesTrainingCompletionDate, referedBy
FROM amtdb.employees
)/*
select distinct
LENGTH(endDate) as byte_length,
    CHAR_LENGTH(endDate) as char_length
#min(endDate), max(endDate)
from employees_cte
#where endDate = ""
order by 1 asc*/
