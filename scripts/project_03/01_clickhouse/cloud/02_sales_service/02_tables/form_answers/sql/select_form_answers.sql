with
form_answers_cte as (
    select *
    from (
        SELECT cdsId,
        questionId,
        answer,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk   
        FROM `sales-service`.form_answers 
        ) where rnk = 1
    )
select * 
from form_answers_cte
where cdsId in ()