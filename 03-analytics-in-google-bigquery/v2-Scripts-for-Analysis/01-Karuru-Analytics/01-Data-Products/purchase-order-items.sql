--------------------- Purchase Order Items ------------------------
with
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where buying_type in ("Purchasing")
                    and workflow_state not in ('CANCELLED', 'REJECTED')
                    and date(creation) >= date_sub(date_trunc(current_date, month), interval 12 month)
                    --WHERE TIMESTAMP_TRUNC(creation, DAY) > TIMESTAMP("2022-02-05")
                    --where date(creation) between '2023-01-01' and  '2023-12-31'
                    --where date(creation) = current_date

                    ),
purchase_order_items as (
                          select distinct date(creation) as purchase_order_creation_date,
                          --date(purchase_order_date) as purchase_order_date,
                          --creation,
                          --modified,
                          --bq_upload_time,
                          --fulfillment_date,
                          po.expiry_date,
                          po.company as company_id,
                          po.territory,
                          po.set_warehouse,
                          po.warehouse_territory as territory_id,
                          po.
                          id,
                          purchase_order_no,
                          --po.buying_type,
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
                          from purchase_order po, unnest(items) i
                          where index =1
                          )
select *
--distinct buying_type, max(creation_date) as max_creation_date
--max(creation) as max_creation, max(modified) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
from purchase_order_items
--and 
where company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
and FORMAT_DATE('%Y%m%d', purchase_order_creation_date) between @DS_START_DATE and @DS_END_DATE
--and territory_id = 'Majengo Mombasa'
--where item_code = 'Taifa Maize Flour 1kg'
--group by 1
--order by 2 desc
--where id = 'PUR-ORD-2024-15080'
--where id = "PUR-ORD-2024-15228"