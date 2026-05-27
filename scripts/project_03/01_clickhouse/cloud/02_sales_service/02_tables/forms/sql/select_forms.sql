WITH
--------------------- Forms ----------------------------------
forms_cte as (
    select *
    from (
        SELECT id,
        name,
        formType,
        status,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
    FROM `sales-service`.forms 
    ) where rnk = 1
    and status = 'active'
    )
select *
--distinct id, name
from forms_cte
ORDER BY 1,2