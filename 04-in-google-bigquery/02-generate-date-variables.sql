WITH 
dates AS (
  SELECT 
    date 
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2020-01-01', DATE_ADD(CURRENT_DATE(), INTERVAL 31 DAY), INTERVAL 1 DAY)) AS date
),
vars AS (
  SELECT 
    DATE('2020-01-01') AS current_start_date, 
    DATE('2020-01-10') AS current_end_date -- Example data, replace with your actual table or CTE
),
vars_array AS (
  SELECT 
    date,
    --FORMAT_DATE('%A',date) as day_of_week
  FROM 
    vars, 
    UNNEST(GENERATE_DATE_ARRAY(current_start_date, current_end_date, INTERVAL 1 DAY)) AS date
)
SELECT *
FROM vars_array;