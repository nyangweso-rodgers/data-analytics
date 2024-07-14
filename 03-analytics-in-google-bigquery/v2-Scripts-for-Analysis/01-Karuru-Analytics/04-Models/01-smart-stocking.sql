---------------------------------- Stock Replienishment Model -----------------------------------
with

territory_mapping as (
                      select distinct original_territory_id,
                      new_territory_id as territory_id,
                      warehouse_name,
                      from `karuru_upload_tables.territory_region_mapping` 
                      ),
--------------------------------- Upload - Supplier Lead Times -----------------------------------
uploaded_territory_supplier_lead_times_table as (
                                                  SELECT distinct company_id, 
                                                  supplier,
                                                  territory_id, 
                                                  safe_cast(suplier_lead_time as int64) as suplier_lead_time
                                                  FROM `kyosk-prod.karuru_upload_tables.territory_supplier_lead_time` 
                                                  ),
--------------------------------------- Opening Stock Balance ---------------------------------
opening_stock_balance as (
                          select distinct opening_balance_date,
                          company_id,
                          warehouse,
                          item_code,
                          stock_uom,
                          sum(qty_after_transaction) as qty_after_transaction
                          FROM `kyosk-prod.karuru_scheduled_queries.opening_stock_balance` 
                          where opening_balance_date >= date_sub(current_date, interval 1 day)
                          group by 1,2,3,4,5
                          ),
opening_stock_balance_with_territories as (
                                            select osb.*,
                                            tm.territory_id,
                                            from opening_stock_balance osb
                                            inner join territory_mapping tm on osb.warehouse = tm.warehouse_name
                                            ),
material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    where date(date_created) >= date_sub(date_trunc(current_date, month), interval 12 month)
                    --where date(date_created) >= '2024-06-01'
                  ),
material_request_items as (
                            select distinct date_created,
                            --mr.date_created as m,
                            --mr.company_id,
                            mr.id,
                            mr.name, 
                            mr.workflow_state,
                            mr.status,
                            --mr.transaction_date,
                            --mr.scheduled_date,
                            mr.target_warehouse_territory_id as territory_id,
                            --mri.territory_id,
                            --mri.item_name,
                            mri.item_name as item_code,
                            mri.item_group as item_group_id,
                            mri.warehouse_id,
                            mri.uom,
                            sum(mri.qty) as ordered_qty,
                            --sum(mri.ordered_qty) as ordered_qty ,
                            --sum(mri.received_qty) as received_qty,
                            --mri.rate,
                            --mri.amount                                
                            from material_request mr, unnest(items) mri
                            where index = 1
                            and material_request_type = 'PURCHASE'
                            group by 1,2,3,4,5,6,7,8,9,10
                            ),
get_pending_material_request as (
                            select distinct --date(date_created) as creation_date_of_material_request,
                            --company_id,
                            territory_id,
                            --warehouse_id,
                            item_code,
                            uom,
                            count(distinct id) as count_pending_material_requests,
                            sum(ordered_qty) as pending_qty_of_material_request,
                            --sum(purchase_receipt_received_qty) as purchase_receipt_received_qty,
                            from material_request_items
                            where workflow_state in ('PENDING', 'VERIFIED', 'SUBMITTED')
                            group by 1,2,3
                            ),
------------------- Purchase Order --------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where date(creation) >= date_sub(date_trunc(current_date, month), interval 12 month)
                    --where date(creation) >= '2024-06-01'
                    ),
purchase_order_items as (
                          select distinct creation,
                          --fulfillment_date,
                          id,
                          --purchase_order_no,
                          i.warehouse_id,
                          i.item_code_id,
                          --i.item_name,
                          --i.qty,
                          --i.stock_qty,
                          --i.item_group
                          from purchase_order po, unnest(items) i
                          where index =1
                          ),
----------------------- Purchase Receipt Item ---------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              where date(date_created) >= date_sub(date_trunc(current_date, month), interval 12 month)
              --where date(date_created) >= '2024-06-01'
              ),
purchase_receipt_items as (
                            select distinct date_created,
                            --posting_date,
                            --posting_time,
                            company_id,
                            pr.set_warehouse_id,
                            territory_id,
                            pr.name,
                            pr.supplier,
                            i.material_request_id,
                            i.purchase_order,
                            workflow_state,
                            i.item_id,
                            --i.item_code,
                            --i.item_name,
                            --i.uom,
                            --i.conversion_factor,
                            i.stock_uom,
                            i.item_group_id,
                            --pri.brand
                            --supplier_name,
                            --avg(rate) as rate,
                            sum(received_qty) as received_qty,
                            --sum(amount) as amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            group by 1,2,3,4,5,6,7,8,9,10,11,12
                            ),
get_item_groups as (
                  select distinct item_id,
                  stock_uom,
                  last_value(item_group_id)over(partition by item_id, stock_uom order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as item_group_id
                  from purchase_receipt_items
                  ),
get_latest_suppliers as (
                        select distinct --company_id,
                        territory_id,
                        set_warehouse_id,
                        item_id,
                        last_value(supplier)over(partition by territory_id, item_id order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_supplier,
                        last_value(date_created)over(partition by territory_id, item_id order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_posting_date
                        from purchase_receipt_items
                        ),
get_latest_purchas_receipt_received_qty as (
                            select distinct date(date_created) as creation_date_of_purchase_receipt,
                            --company_id,
                            --warehouse_id,
                            territory_id,
                            item_id,
                            stock_uom, 
                            sum(received_qty) as purchase_receipt_received_qty,
                            from purchase_receipt_items
                            where workflow_state in ('COMPLETED')
                            group by 1,2,3,4
                            ),
--------------------------- Purchasing cte ---------------------------
purchasing_cte as (
                    select distinct date(mri.date_created) as creation_date_of_material_request,
                    date(poi.creation) as creation_date_of_purchase_order,
                    date(pri.date_created) as creation_date_of_purchase_receipt,
                    date_diff(date(pri.date_created), date(poi.creation), day) as calculated_lead_time,
                    --mri.company_id,
                    mri.territory_id,
                    mri.warehouse_id,
                    mri.id as material_request_id,
                    --mri.name,
                    pri.purchase_order,
                    pri.name as purchase_receipt,
                    mri.workflow_state as workflow_state_of_material_request,
                    pri.workflow_state as workflow_state_of_purchase_receipt,
                    mri.status as status_of_material_request,
                    mri.item_group_id,
                    --mri.item_name,
                    mri.item_code,
                    mri.uom,
                    mri.ordered_qty as ordered_qty_of_material_request,
                    --mri.received_qty as material_request_received_qty,
                    pri.received_qty as purchase_receipt_received_qty,
                    pri.supplier
                    from material_request_items mri
                    left join purchase_receipt_items pri on mri.warehouse_id = pri.set_warehouse_id and  mri.id = pri.material_request_id and mri.item_code = pri.item_id and mri.uom = pri.stock_uom
                    left join purchase_order_items poi on poi.id = pri.purchase_order and poi.warehouse_id = pri.set_warehouse_id and poi.item_code_id = pri.item_id
                    ),
pending_and_completed_purchasing_report as (
                                            select *
                                            FROM purchasing_cte
                                            WHERE workflow_state_of_purchase_receipt not in ('REJECTED') 
                                            ),
get_avg_lead_time as (
                  select distinct --company_id,
                  warehouse_id,
                  territory_id,
                  supplier,
                  round(avg(calculated_lead_time)over(partition by territory_id, supplier order by creation_date_of_purchase_receipt asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as calculated_lead_time
                  from pending_and_completed_purchasing_report
                  ),
----------------------------------------- Demand - Delivery Notes ---------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at) > date_sub(current_date, interval 4 month)
                --and date(created_at) > '2023-08-05'
                ),
delivery_notes_items as (
                        select dn.delivery_window.delivery_date as scheduled_delivery_date,
                        date(dn.delivery_date) as delivery_date,
                        country_code,
                        dn.territory_id,
                        oi.product_bundle_id, 
                        oi.inventory_items 
                        from delivery_notes dn, unnest(order_items) oi
                        where index = 1
                        --and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                        --and oi.status = 'ITEM_FULFILLED'
                        ),
delivery_notes_inventory_items as (
                                  select distinct  
                                  dn.country_code,
                                  dn.territory_id,
                                  scheduled_delivery_date,
                                  --dn.product_bundle_id,
                                  ii.stock_item_id,
                                  ii.uom as stock_uom,
                                  sum(ii.inventory_item_qty) as inventory_item_qty
                                  from delivery_notes_items dn, unnest(inventory_items) ii 
                                  group by 1,2,3,4,5
                                  ),
dns_items_qty as (
                    select *,
                    row_number()over(partition by territory_id, stock_item_id,stock_uom order by scheduled_delivery_date asc) as scheduled_delivery_date_index,
                    sum(inventory_item_qty) over(partition by territory_id, stock_item_id,stock_uom order by scheduled_delivery_date asc) as inventory_item_total_qty
                    from delivery_notes_inventory_items
                    order by territory_id, stock_item_id, scheduled_delivery_date
                    ),
--------------------------------- Model ------------------------------------------------
opening_balance_with_purchases as (
  select distinct osbwt.opening_balance_date,
  osbwt.company_id,
  osbwt.territory_id,
  osbwt.warehouse,
  osbwt.item_code,
  osbwt.stock_uom,
  gig.item_group_id,
  osbwt.qty_after_transaction as opening_stock_qty,
  osbwt.qty_after_transaction + coalesce(gprirq.purchase_receipt_received_qty,0) as adjusted_stock_qty,
  coalesce(gprirq.purchase_receipt_received_qty,0) as purchase_receipt_received_qty,
  gls.latest_supplier,
  date(gls.latest_posting_date) as latest_posting_date,
  utsltt.suplier_lead_time as supplier_lead_time,
  safe_cast(galt.calculated_lead_time as int64) as calculated_lead_time,
  coalesce(utsltt.suplier_lead_time, safe_cast(galt.calculated_lead_time as int64)) + 1 as adjusted_lead_time,
  case
    when gls.latest_posting_date is null then 'NO' 
  else 'YES' end as has_historical_purchase_receipt,
  coalesce(gpmr.count_pending_material_requests, 0) as count_pending_material_requests,
  coalesce(gpmr.pending_qty_of_material_request, 0) as pending_qty_of_material_request
  from opening_stock_balance_with_territories osbwt
  left join get_item_groups gig on osbwt.item_code = gig.item_id and osbwt.stock_uom = gig.stock_uom
  left join get_latest_suppliers gls on osbwt.territory_id = gls.territory_id and osbwt.item_code = gls.item_id
  left join get_latest_purchas_receipt_received_qty gprirq on osbwt.territory_id = gprirq.territory_id and osbwt.item_code = gprirq.item_id and osbwt.stock_uom = gprirq.stock_uom and osbwt.opening_balance_date = gprirq.creation_date_of_purchase_receipt
  left join uploaded_territory_supplier_lead_times_table utsltt on osbwt.territory_id = utsltt.territory_id and utsltt.supplier = gls.latest_supplier
  left join get_avg_lead_time galt on osbwt.territory_id = galt.territory_id and gls.latest_supplier = galt.supplier
  
  left join get_pending_material_request gpmr on osbwt.territory_id = gpmr.territory_id and osbwt.item_code = gpmr.item_code and osbwt.stock_uom = gpmr.uom
  ),
calculate_daily_demand as (
              select obwp.*,
              case
                when has_historical_purchase_receipt = 'NO' then 'Not Stocked'
                when (has_historical_purchase_receipt = 'YES') and (adjusted_stock_qty <= 3) then 'Out Of Stock'
                when (has_historical_purchase_receipt = 'YES') and (adjusted_stock_qty > 3) then 'In Stock'
              else 'UNRECOGNIZED' end as sku_stock_status,
              dniq.inventory_item_total_qty as dn_item_order_qty,
              safe_divide(dniq.inventory_item_total_qty,adjusted_lead_time)  as dn_item_daily_demand_qty,
              opening_stock_qty / safe_divide(dniq.inventory_item_total_qty,adjusted_lead_time) as stock_cover_days
              --safe_divide()
              from opening_balance_with_purchases obwp
              left join dns_items_qty dniq on obwp.territory_id = dniq.territory_id and obwp.item_code = dniq.stock_item_id and obwp.stock_uom = dniq.stock_uom and obwp.adjusted_lead_time = dniq.scheduled_delivery_date_index
              )
select *
--distinct out_of_stock_status
from calculate_daily_demand
where territory_id not in ('Karatina','Nakuru', 'Kyosk HQ', 'Test KE Territory')
and FORMAT_DATE('%Y%m%d', opening_balance_date) between @DS_START_DATE and @DS_END_DATE
--and opening_balance_date = current_date 
and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
--and territory_id = 'Voi'
--and territory_id= 'Nyeri'
--and item_code = 'Sunsalt Salt 500G'
--order by item_code, warehous