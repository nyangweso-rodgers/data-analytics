--------------------------Product Bundle - Catalog SKUs-------------------------------------
with

product_bundle as (
                    select *,
                    row_number()over(partition by id order by modified desc) as index
                    from `kyosk-prod.karuru_reports.product_bundle` 
                    where date(creation) >= '2020-01-01'
                    and disabled = false
                    ),
product_bundle_report as (
                        select distinct pb.creation,
                        pb.modified,
                        pb.bq_upload_time,
                        pb.id,
                        pb.non_stock_item_id
                        uom,
                        pb.description,
                        pb.item_group_id,
                        --si.stock_item_id,
                        --si.stock_uom
                        pb.disabled,
                        dimension.metric as dimesnsion_metric,
                        dimension.shape as dimension_shape,
                        from product_bundle pb,unnest(stock_items) si
                        where index = 1
                        )
select *
from product_bundle_report 