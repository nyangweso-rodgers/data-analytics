with
outlets as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.outlets` 
            WHERE date(created_at) >= '2022-02-01'
            ),
outlets_lists as (
                  select distinct market.company, 
                  id,
                  retailer_id,
                  name,
                  erp_id,
                  app_created_on,
                  latitude,
                  longitude,
                  market_id,
                  route_id
                  from outlets
                  where index =1
                  )
select distinct id as outlet_id,
route_id
from outlets_lists
where company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
--id = '0F2MYQ3687VNQ'