# BigQuery Window/Analytics Functions

## Table of Contents

- [Further Reading]()
  1. [cloud.google.com/biguery/docs/reference - Window function calls](https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls)
  2. [https://nyangweso-rodgers.hashnode.dev/ - Analytics Functions in Google BigQuery](https://nyangweso-rodgers.hashnode.dev/analytics-functions-in-google-bigquery)

# Introduction to Analytics Functions

- A **window(analytic) function** computes values over a group of rows and returns a single result for each row. This is different from an **aggregate function**, which returns a single result for a group of rows.
- A **window function** includes an `OVER` clause, which defines a window of rows around the row being evaluated. For each row, the window function result is computed using the selected window of rows as input, possibly doing aggregation.
- With **window functions** you can compute the following:
  1. moving averages,
  2. rank items,
  3. calculate cumulative sums, e.t.c.,

# Sample BigQuery Table

- Let's create a simple `sale_orders` table inside the `sales` dataset:

  ```sql
    -------------- Create a sales_order table --------------
  with
  sales_order as (
                select DATE('2023-11-07') as delivery_date, 'Customer A' as customer, 'Kenya' as shipping_country, 10 as amount
                union all select DATE('2023-11-08') as delivery_date, 'Customer A' as customer, 'Kenya' as shipping_country, 20 as amount
                union all select DATE('2023-11-09') as delivery_date, 'Customer A' as customer, 'Kenya' as shipping_country, 30 as amount
                union all select DATE('2023-11-10') as delivery_date, 'Customer A' as customer, 'Kenya' as shipping_country, 40 as amount
                union all select DATE('2023-11-08') as delivery_date, 'Customer B' as customer, 'Kenya' as shipping_country, 100 as amount
                union all select DATE('2023-11-09') as delivery_date, 'Customer B' as customer, 'Kenya' as shipping_country, 200 as amount
                union all select DATE('2023-11-10') as delivery_date, 'Customer B' as customer, 'Kenya' as shipping_country, 300 as amount
                )
  select * from sales_order
  ```



# `FIRST_VALUE`

- `FIRST_VALUE` returns a value for the first row in the current window frame.
- **Use case**:
  - Suppose we are interested in getting unique list of customers together with their first `delivery_date` from the `sales_order`?
  - We could simply use the `FIRST_VALUE` function as follows:
    ```sql
        ------- get customer list with last delivery date
        select
            distinct customer,
            FIRST_VALUE(delivery_date) OVER (PARTITION BY customer ORDER BY delivery_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_delivery_date
        from sales_order
    ```

# `LAST_VALUE`

- `LAST_VALUE` returns value of the last row in the current window frame.
- **Use case**:

  - Suppose we are interested in getting unique list of customers together with their last `delivery_date` from the `sales_order`?
  - We could simply use the `LAST VALUE` function as follows:

    ```sql
        ------- get customer list with last delivery date
        select
            distinct customer,
            LAST_VALUE(delivery_date) OVER (PARTITION BY customer ORDER BY delivery_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_delivery_date
        from sales_order
    ```

    Output:

    ![Alt text](image-1.png)
