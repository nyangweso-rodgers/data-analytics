WITH
--------------------- Forms ----------------------------------
forms_cte as (
    select *
    from (
        SELECT id,
        name,
        formTypeId,
        formType,
        status,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
    FROM `sales-service`.forms 
    ) where rnk = 1
    and status = 'active'
    ),
--------------------- Form Types ----------------------------------
form_types_cte as (
    select *
    from (
        SELECT id,
        name,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM `sales-service`.form_types
    ) where rnk =1
    ),
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
    ),
--------------------- Forms Mashup ----------------------------------
forms_mashup_cte as (
    select forms_cte.id as form_id,
    forms_cte.name as form_name,
    forms_cte.formType,
    form_types_cte.name,
    forms_cte.status,
    form_sections_cte.name as form_section
    from forms_cte
    left join form_types_cte on toInt64(form_types_cte.id) = forms_cte.formTypeId
    left join form_sections_cte on toInt64(form_sections_cte.formId) = forms_cte.id
    ORDER BY form_id, form_section
    )
select *
--distinct id, name
--from forms_cte
from forms_mashup_cte