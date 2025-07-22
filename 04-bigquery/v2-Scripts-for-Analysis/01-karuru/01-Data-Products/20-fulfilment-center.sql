---------------------- Fulfilment Center ------------------------
with
fulfillment_center as (
                      SELECT *,
                      row_number()over(partition by id order by updated_at desc) as index 
                      FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                      where name not in ('Test FC', 'Kyosk HQ', 'Test254', 'Kyosk TZ HQ', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ - UG', 'Test Fresh TZ Territory', 'Test TZ Territory', 
                      'Test NG Territory', 'Kyosk South West HQ')
                      and (location.latitude is not null)
                      and date(created_at) > "2021-06-27" #start date
                      --and country_code = 'KE'
                      --and name in ('Khetia ', 'Ruiru')
                      and id = '0HNP721RAVN7E'
                      ),
fulfillment_center_cte as (
                            select distinct --date(created_at) created_at,
                            id,
                            name,
                            country_code,
                            cast(location.latitude as float64) as latitude,
                            cast(location.longitude as float64) as longitude
                            from fulfillment_center
                            where index =1 
                            )
select *
from fulfillment_center_cte

--where id in ('0FST5TTHJHQZ3')
order by 3,2