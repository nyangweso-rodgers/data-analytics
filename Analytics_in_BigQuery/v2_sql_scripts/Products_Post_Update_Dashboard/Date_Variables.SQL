with
dates as (SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2022-03-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date),
vars AS (
  --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  SELECT DATE '2022-10-31' as current_start_date, DATE '2022-11-06' as current_end_date ),
date_vars as (
                select *,

                date_trunc(current_start_date, week(MONDAY)) as current_start_week,
                date_add(date_trunc(current_start_date, week(MONDAY)), interval 6 day) as current_end_week,

                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 week) as previous_start_week, 
                date_sub(date_trunc(current_start_date, week(MONDAY)), interval 1 day) as previous_end_week ,

                from vars
                )

select * from date_vars