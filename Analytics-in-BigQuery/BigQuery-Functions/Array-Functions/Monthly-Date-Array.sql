-------------------------- Monthly Date Array ----------------------
with
date_array as (
              SELECT * 
              FROM  UNNEST(GENERATE_DATE_ARRAY(
                '2022-03-01',
                date_add(CURRENT_DATE(),interval 31 day), 
                INTERVAL 1 DAY)) AS my_date
              ),
month_array as (
                SELECT DISTINCT DATE_TRUNC(my_date, MONTH) as my_month,
                count(distinct my_date) as calendar_days,
                count(distinct (case when FORMAT_DATE('%A',my_date) not in  ('Sunday') then my_date else null end )) as sale_days
                from date_array
                group by 1
                )

select * from month_array