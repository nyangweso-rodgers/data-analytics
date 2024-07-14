---------------------- Fulfilment Center ------------------------
with
fulfillment_center as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                        WHERE date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_list as (
                            select distinct --date(created_at) created_at,
                            id,
                            name,
                            country_code
                            from fulfillment_center
                            where index =1 
                            )
select *
from fulfillment_center_list
where name not in ('Kyosk HQ', 'Test254', 'Kyosk TZ HQ', 'Test UG Territory', 'Test KE Territory')
--where id in ('0FST5TTHJHQZ3')
order by 3,2