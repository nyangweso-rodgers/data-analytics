----------------- Market Activators ----------
with
market_activators as (
                      select *,
                      row_number()over(partition by id order by bq_upload_time desc) as index
                      from `karuru_reports.market_activators`
                      where date(created_at) >= '2023-08-01'
                    ),
market_activators_cte as (
                          select distinct created_at,
                          id,
                          names,
                          email
                          msisdn,
                          market_id,
                          active
                          from market_activators
                          where index = 1
                          )
select * from market_activators_cte