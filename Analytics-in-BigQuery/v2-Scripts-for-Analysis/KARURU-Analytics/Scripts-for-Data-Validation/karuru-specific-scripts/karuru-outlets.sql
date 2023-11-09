----------------- Karuru ------------
-------------Outlets ---------------
with
karuru_outlets as (
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index
                  FROM `kyosk-prod.karuru_reports.outlets` 
                  WHERE date(created_at) > '2022-01-01'
                  ),
karuru_outlets_lists as (
                        SELECT distinct date(created_at) as created_at,
                        id,
                        market.market_name as market_name,
                        FROM karuru_outlets--.market
                        --left join unnest(market) as market
                        WHERE index = 1
                        )
select *
from karuru_outlets_lists
where (market_name is null)
and (market_name not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory'))