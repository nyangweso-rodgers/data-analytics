--------------------------------------- Purchase Order & Purchase Receipt Items ---------------------------
with
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where buying_type in ("Purchasing")
                    --WHERE TIMESTAMP_TRUNC(creation, DAY) > TIMESTAMP("2022-02-05")
                    and date(creation) >= date_sub(date_trunc(current_date, month), interval 12 month)
                    --where date(creation) between '2024-06-01' and '2024-06-30' 
                    ),
purchase_order_items as (
                          select distinct date(creation) as purchase_order_creation_date,
                          --date(purchase_order_date) as purchase_order_creation_date,
                          creation as purchase_order_creation_datetime,
                          --modified,
                          --bq_upload_time,
                          --fulfillment_date,
                          --po.expiry_date,
                          po.company,
                          po.territory,
                          --po.set_warehouse,
                          po.warehouse_territory,
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
                          ),
------------------------------ Purchase Receipt ---------------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              where buying_type in ('PURCHASING')
              and date(date_created) >= date_sub(date_trunc(current_date, month), interval 12 month)
              --where date(date_created) >= '2022-01-01'
              ),
purchase_receipt_items as (
                            select distinct date(date_created) as purchase_receipt_creation_date,
                            --date_created as purchase_receipt_creation_datetime,
                            posting_date,
                            company_id,
                            territory_id,
                            name,
                            pr.workflow_state,
                            i.purchase_order,
                            i.item_id,
                            i.item_code,
                            i.item_name,
                            --i.uom,
                            --i.conversion_factor,
                            i.stock_uom,
                            --pri.item_group_id,
                            --pri.brand
                            supplier,
                            --supplier_name,
                            --avg(rate) as rate,
                            received_qty,
                            --sum(amount) as amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            --group by 1,2,3,4,5,6,7,8,9,10,11,12
                            ),
purchase_order_and_purchase_receipt as (
                                        select distinct poi.purchase_order_creation_date,
                                        pri.purchase_receipt_creation_date,
                                        date_diff(date(pri.purchase_receipt_creation_date), date(poi.purchase_order_creation_date), day) as purchase_order_to_purchase_receipt_lead_time,
                                        pri.posting_date as posting_date_of_purchase_receipt,
                                        poi.company as company_id,
                                        poi.warehouse_id,
                                        --poi.set_warehouse,
                                        poi.warehouse_territory as territory_id,
                                        poi.id as purchase_order_id,
                                        poi.purchase_order_no,
                                        poi.workflow_state as workflow_state_of_purchase_order,
                                        pri.name as purchase_receipt_name,
                                        pri.workflow_state as workflow_state_of_purchase_receipt,
                                        poi.item_group,
                                        poi.item_code_id as item_code,
                                        poi.item_name,
                                        poi.stock_uom,
                                        poi.qty as qty_of_purchase_order,
                                        --poi.stock_qty as purchase_order_stock_qty,
                                        pri.received_qty as purchase_receipt_received_qty,
                                        --pri.amount as purchase_receipt_amount,
                                        coalesce(poi.supplier, pri.supplier) as supplier,
                                        --coalesce(poi.supplier_name, pri.supplier_name) as supplier_name,
                                        from purchase_order_items poi
                                        left join purchase_receipt_items pri on poi.purchase_order_no = pri.purchase_order and poi.item_code_id = pri.item_code and poi.stock_uom = pri.stock_uom
                                        ),
get_item_groups as (
                  select distinct item_code,
                  stock_uom,
                  last_value(item_group)over(partition by item_code order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as item_group_id
                  from purchase_order_and_purchase_receipt
                  ),
get_pending_purchase_order_and_purchase_orders as (
                                                  select distinct company_id,
                                                  territory_id,
                                                  item_code,
                                                  stock_uom,
                                                  count(distinct purchase_order_id) as count_of_pending_purchase_order,
                                                  sum(qty_of_purchase_order) as qty_of_pending_purchase_order
                                                  from purchase_order_and_purchase_receipt
                                                  where workflow_state_of_purchase_order in ('PENDING', 'SUBMITTED')
                                                  group by 1,2,3,4
                                                  ),
get_completed_purchase_order_and_purchase_receipts as (
                                                      select *
                                                      from purchase_order_and_purchase_receipt
                                                      where workflow_state_of_purchase_receipt  in ('COMPLETED')
                                                      ),
get_latest_suppliers as (
  select distinct 
  company_id,
  --warehouse_id,
  territory_id,
  item_code,
  last_value(supplier)over(partition by territory_id, item_code order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_supplier,
  last_value(posting_date_of_purchase_receipt)over(partition by territory_id, item_code order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_posting_date
  from get_completed_purchase_order_and_purchase_receipts
  ),
get_daily_purchase_receipt_received_qty as (
                            select distinct posting_date_of_purchase_receipt,
                            --company_id,
                            territory_id,
                            warehouse_id,
                            item_code,
                            stock_uom, 
                            sum(purchase_receipt_received_qty) as purchase_receipt_received_qty,
                            from get_completed_purchase_order_and_purchase_receipts
                            group by 1,2,3,4,5
                            ),
get_purchase_order_to_purchase_receipt_avg_lead_time as (
      select distinct company_id,
      warehouse_id,
      territory_id,
      supplier,
      round(avg(purchase_order_to_purchase_receipt_lead_time)over(partition by territory_id, supplier order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as      
      calculated_lead_time
      from get_completed_purchase_order_and_purchase_receipts
      )
select *
--distinct worflow_state_of_purchase_order, workflow_state_of_purchase_receipt
from purchase_order_and_purchase_receipt
--from purchase_order_and_purchase_receipt
--from purchase_order_items
--from get_pending_purchase_orders
--from get_latest_suppliers
where company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
--where company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
and territory_id = 'Majengo Mombasa'
--and purchase_order_no = 'PUR-ORD-2024-13650'
--and item_code  in ('Zuri Packed Sugar 1kg', 'Taifa Maize Flour 1kg')
and purchase_order_no = "PUR-ORD-2024-15228"