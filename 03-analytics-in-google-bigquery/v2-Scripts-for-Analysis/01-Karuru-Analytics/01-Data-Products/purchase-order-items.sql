--------------------- Purchase Order Items ------------------------
with
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where date(creation) > date_sub(current_date, interval 4 year)
                    --WHERE TIMESTAMP_TRUNC(creation, DAY) > TIMESTAMP("2022-02-05")
                    --where date(creation) between '2023-01-01' and  '2023-12-31'
                    --where date(creation) = current_date
                    ),
purchase_order_items as (
                          select distinct date(creation) as creation_date,
                          date(purchase_order_date) as purchase_order_date,
                          --creation,
                          --modified,
                          --bq_upload_time,
                          fulfillment_date,
                          po.expiry_date,
                          po.company,
                          po.territory,
                          po.set_warehouse,
                          po.warehouse_territory,
                          po.
                          id,
                          purchase_order_no,
                          po.buying_type,
                          po.workflow_state,
                          po.supplier,
                          po.supplier_name,
                          i.warehouse_id,
                          i.item_code_id,
                          i.item_name,
                          i.item_group,
                          i.uom,
                          i.stock_uom,

                          i.qty,
                          i.stock_qty,
                          i.item_group
                          from purchase_order po, unnest(items) i
                          where index =1
                          )
select *
--distinct buying_type, max(creation_date) as max_creation_date
--max(creation) as max_creation, max(modified) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
from purchase_order_items
where buying_type in ("Purchasing")
and workflow_state not in ('CANCELLED', 'REJECTED')
and supplier is null
--group by 1
--order by 2 desc