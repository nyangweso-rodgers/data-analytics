-------------------- Outlets ------------------------------
with
outlets as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.outlets` 
            WHERE date(created_at) >= '2022-02-01'
            --and id = '0CWRTG5N1CTJJ'
            --and id  = '0H573S109T1XZ'
            ),
outlets_cte as (
                  select distinct date_trunc(date(created_at), month) as created_at_month,
                  created_at as created_at_datetime,
                  date(created_at) as created_at_date,
                  updated_at,
                  created_by,
                  updated_by,
                  market.company as company_id, 
                  market.market_name as market_name,
                  --market.territory as territory, # all null
                  route_id,
                  id,
                  market_developer.first_name as market_developer_first_name,
                  market_developer.last_name as market_developer_last_name
                  --retailer_id,
                  --name,
                  --erp_id,
                  --app_created_on,
                  --latitude,
                  --longitude,
                 --market_id,
                  from outlets
                  where index =1
                  and (market.market_name is not null) 
                  and market.market_name not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
                  ),
outlets_agg_cte as (
                    select distinct company_id,
                    market_name, 
                    count(distinct id) as count_of_outlets
                    from outlets_cte
                    --where (market_name is not null) 
                    --and market_name not in ('Kyosk TZ HQ', 'Test TZ Territory', 'Test Fresh TZ Territory', 'Test UG Territory', 'Test KE Territory', 'Kyosk HQ', 'Test NG Territory')
                    group by 1,2
                    order by 1,3 desc,2
                    )
select *
from outlets_agg_cte
--where company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
--order by created_at_date desc