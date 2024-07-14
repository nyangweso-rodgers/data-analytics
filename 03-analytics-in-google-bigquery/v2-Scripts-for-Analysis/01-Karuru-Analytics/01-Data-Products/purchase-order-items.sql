with
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    --WHERE TIMESTAMP_TRUNC(creation, DAY) > TIMESTAMP("2022-02-05")
                    where date(creation) between '2023-01-01' and  '2023-12-31'
                    ),
purchase_order_items as (
                          select distinct date(creation) as creation,
                          date(purchase_order_date) as purchase_order_date,
                          fulfillment_date,
                          id,
                          purchase_order_no,
                          i.warehouse_id,
                          i.item_code_id,
                          i.item_name,
                          i.qty,
                          i.stock_qty,
                          i.item_group
                          from purchase_order po, unnest(items) i
                          where index =1
                          )
select 
count(distinct id)
from purchase_order_items