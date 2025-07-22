----------------------- promotions ------------------
with
promotions as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index 
                FROM `kyosk-prod.karuru_reports.promotions` 
                WHERE date(created_at) > "2023-09-01" 
                ),
promotions_cte as (
                    select distinct created_at as created_at_datetime,
                    last_modified as last_modified_datetime,
                    start_date,
                    end_date,
                    country_code,
                    --p.territory_id,
                    customer_segment,
                    is_active,
                    approved,
                    id,
                    name,
                    app,
                    status,
                    promotion_on,
                    promotion_type,
                    pi.territory_id,
                    pi.catalog_item_id,
                    pi.funding,
                    pi.supplier,
                    pi.item_code,
                    pi.item_name,
                    pi.uom,
                    pi.discount_type,
                    pi.discount_amount,
                    pi.discount_percentage
                    from promotions p, unnest(promo_items) pi
                    where index =1
                    )
select distinct country_code,territory_id, count(distinct name)
--min(created_at_datetime) as min_created_at_datetime, min(last_modified_datetime) as min_last_modified_datetime,
--max(created_at_datetime) as max_created_at_datetime, max(last_modified_datetime) as max_last_modified_datetime
from promotions_cte
where territory_id not in ('Test254', 'Test FC', 'bin', 'new', 'Test KE Territory', 'Test UG Territory')
and country_code in ('KE')
group by 1,2
order by 1,3 desc