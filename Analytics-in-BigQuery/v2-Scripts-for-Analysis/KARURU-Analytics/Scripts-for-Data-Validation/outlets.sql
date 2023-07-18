----------------- Karuru - Outlets ---------------
with
outlets_with_index as (
                        SELECT distinct date(created_at) as created_at,
                        date(updated_at)  as updated_at,
                        id,
                        erp_id,
                        name,
                        outlet_code,
                        market.company,
                        market.market_name,
                        created_by,
                        updated_by,
                        market_developer.first_name,
                        market_developer.last_name,
                        market_developer.phone_number,
                        row_number()over(partition by name order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.outlets` 
                        WHERE date(created_at) >= '2022-02-01'
                        and market.market_name not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                        --and outlet_code = 'BJXO'
                        --and erp_id = "WGKP-Namata Shop00001"
                        )

select *
from outlets_with_index
where index = 1
and FORMAT_DATE('%Y%m%d', created_at) between @DS_START_DATE and @DS_END_DATE
order by 1,2