# Looker Studio Functions

## Table Of Contents
- [References]()
    - [Function list](https://support.google.com/looker-studio/table/6379764?hl=en)
    - [Count Distinct with Condition in Data Studio [NEW]](https://danalyser.com/blogs/google-data-studio/count-distinct-with-condition-in-data-studio-new)

# Description
* Popular Looker Studio Functions and their usage.

# Popular Looker Studio Functions

# `case when` function
```sql
    -- count number of order delivered early against the total order delivered
    COUNT_DISTINCT(case when otif_status = 'EARLY' then order_id else null end) / COUNT_DISTINCT(order_id)
```