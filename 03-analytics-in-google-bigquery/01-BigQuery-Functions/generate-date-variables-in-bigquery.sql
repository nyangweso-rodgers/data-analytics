---------------------- Dynamic Date Variables for SQL Scripts ----------------------
with 
dates as (
          SELECT * FROM  UNNEST(GENERATE_DATE_ARRAY('2020-01-01',date_add(CURRENT_DATE(),interval 31 day), INTERVAL 1 DAY)) AS date
          ),
vars AS (
 --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) end  as current_end_date ),
  SELECT DATE '2024-09-10' as current_start_date,  DATE '2021-09-10' as current_end_date ),

date_vars as (  
              select *,
                date_sub(current_start_date, interval 7 day) as previous_seven_day_start_date,
                date_sub(current_start_date, interval 1 day) as previous_seven_day_end_date,
                date_sub(date_trunc(current_start_date, month), interval 1 month) as previous_start_month,
                date_sub(date_trunc(current_start_date, month), interval 1 day) as previous_end_month,
              from vars
                )
              
select * from date_vars