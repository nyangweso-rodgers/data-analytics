------------------------- Test Script - Kyos+ , IT ACCESSORIES -----------------------
with
front_margin_raw as (
                      SELECT distinct creation_date,
                      company,
                      sum(total_incoming_rate) as total_incoming_rate,
                      sum(base_net_amount_of_sales_invoice) - sum(total_incoming_rate) as margin_ampunt
                      FROM `kyosk-prod.erp_scheduled_queries.erp_front_margin_v2` 
                      where item_group in ('IT Accessories','IT Accessories.')
                      group by 1,2
                      )
front_margin as (
                  select *
                  from front_margin_raw, 
                  )