with
customer_dimension_cte as (
                          SELECT distinct dimension_group,
                          ARRAY_AGG(distinct dimension order by dimension) as dimensions
                          FROM `kyosk-prod.karuru_scheduled_queries.customer_dimension` 
                          group by 1
                          --order by 1
                          )
select GENERATE_UUID() as id, * from customer_dimension_cte