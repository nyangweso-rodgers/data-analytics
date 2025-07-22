with
slim_delivery_notes as (
                        SELECT distinct date(created_at) as created_at,
                        id,
                        code,
                        outlet_id,
                        territory_id,
                        country_code,
                        row_number()over(partition by id order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.slim_delivery_notes` 
                        --WHERE date(created_at) >= ("2024-07-04") 
                        where date(created_at) between '2024-06-01' and '2024-06-30'
                        )
select 
distinct date_trunc(created_at, month) as created_month, count(distinct id) as count_id, count(distinct code) as count_, count(distinct outlet_id) as count_outlet_id
from slim_delivery_notes
where index =1 
group by 1