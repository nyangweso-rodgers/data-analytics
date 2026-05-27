WITH
--------------------- Form Sections ----------------------------------
form_sections_cte as (
    select *
    from (
        SELECT id,
        formId,
        name,
        formType,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.form_sections
    ) where rnk = 1
    )
select --*
distinct formId, formType
from form_sections_cte
ORDER BY 1,2