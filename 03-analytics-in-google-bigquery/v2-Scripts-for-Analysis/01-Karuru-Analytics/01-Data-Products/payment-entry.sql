with
payment_entry as (
                  SELECT *,
                  row_number()over(partition by id  order by modified desc) as index
                  FROM `kyosk-prod.karuru_reports.payment_entry` 
                  WHERE date(creation) > "2021-06-26"
                  )
select min(creation)
from payment_entry
where index =1