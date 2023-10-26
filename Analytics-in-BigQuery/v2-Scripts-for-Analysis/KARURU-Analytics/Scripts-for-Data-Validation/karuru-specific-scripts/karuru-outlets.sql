----------------- Karuru ------------
-------------Outlets ---------------
with
outlets_with_index as (
                        SELECT distinct date(created_at) as created_at,
                        id,
                        retailer_id,
                        erp_id,
                        name,
                        outlet_code,
                        --market.company,
                        market.market_name,
                        row_number()over(partition by id order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.outlets` 
                        WHERE date(created_at) > '2022-01-01'
                        and market.market_name not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                        )
select distinct id,retailer_id, erp_id
from outlets_with_index
where index = 1