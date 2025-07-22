# Array Functions in BigQuery

## Table Of Contents
- [Further Reading]()
  - [cloud.google.com - array-functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions)

# GENERATE_DATE_ARRAY
* Syntax and example
    ```sql
        -- syntax
        GENERATE_DATE_ARRAY(start_date, end_date[, INTERVAL INT64_expr date_part])

        -- example - without interval
        select GENERATE_DATE_ARRAY(date '2023-06-01', date '2023-06-30') as the_date

        -- 2 days step
        select GENERATE_DATE_ARRAY(date '2023-06-01', date '2023-06-30', interval 2 day) as the_date

        -- monthly DATE Arrays
        select * from unnest(GENERATE_DATE_ARRAY(date '2022-04-01', date_trunc(current_date,month), interval 1 MONTH)) as the_month
    ```

* `start_date` and `end_date` parameters determine the inclusive start and end of the array.
* The `GENERATE_DATE_ARRAY` function accepts the following data types as inputs: 
  * `start_date` must be a `DATE`.
  * `end_date` must be a `DATE`.
  * `INT64_expr` must be an `INT64`.
  * `date_part` must be either `DAY`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.