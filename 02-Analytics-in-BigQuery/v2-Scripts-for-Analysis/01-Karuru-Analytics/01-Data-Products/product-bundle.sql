--------------------------Product Bundle QA - Catalog SKUs-------------------------------------
with

product_bundle as (
                    select *,
                    row_number()over(partition by id order by modified desc) as index
                    from `kyosk-prod.karuru_reports.product_bundle` 
                    where date(creation) >= '2020-01-01'
                    and disabled = false
                    ),
product_bundle_report as (
                        select distinct pb.id,
                        --uom,
                        item_group_id,
                        si.stock_item_id,
                        si.stock_uom
                        from product_bundle pb,unnest(stock_items) si
                        where index = 1
                        )
select *
from product_bundle_report 