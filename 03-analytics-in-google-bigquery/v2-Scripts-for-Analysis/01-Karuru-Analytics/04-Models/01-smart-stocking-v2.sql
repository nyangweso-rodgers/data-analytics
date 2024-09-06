---------------------------------- Stock Replienishment Model - v2 -----------------------------------
with

territory_mapping as (
                      select distinct original_territory_id,
                      new_territory_id,
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
                          date_sub(date_trunc(opening_balance_date,week(monday)), interval 4 week) as four_week_demand_plan_start_date,
                          date_sub(date_trunc(opening_balance_date,week(monday)), interval 1 day)  as four_week_demand_plan_end_date,
                          company_id,
                          warehouse,
                          item_code,
                          stock_uom,
                          sum(qty_after_transaction) as qty_after_transaction
                          FROM `kyosk-prod.karuru_scheduled_queries.opening_stock_balance` 
                          where opening_balance_date >= date_sub(current_date, interval 1 day)
                          group by 1,2,3,4,5,6,7
                          ),
opening_stock_balance_with_territories as (
                                            select osb.*,
                                            tm.original_territory_id,
                                            tm.new_territory_id,
                                            from opening_stock_balance osb
                                            inner join territory_mapping tm on osb.warehouse = tm.warehouse_name
                                            ),
--------------------- Material Requests ---------------------------------------------------------------
material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    where date(date_created) >= date_sub(date_trunc(current_date, month), interval 3 month)
                    and material_request_type = 'PURCHASE'
                    and workflow_state not in ('REJECTED')
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
                            mr.target_warehouse_territory_id,
                            i.territory_id,
                            --mri.item_name,
                            i.item_code,
                            i.item_name,
                            i.item_group,
                            i.warehouse_id,
                            i.uom,
                            i.qty
                            --sum(mri.qty) as ordered_qty,
                            --sum(mri.ordered_qty) as ordered_qty ,
                            --sum(mri.received_qty) as received_qty,
                            --mri.rate,
                            --mri.amount                                
                            from material_request mr, unnest(items) i
                            where index = 1
                            ),
------------------- Purchase Order --------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where buying_type in ("Purchasing")
                    and date(creation) >= date_sub(date_trunc(current_date, month), interval 12 month)
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

                          po.id,
                          i.material_request,
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
----------------------- Purchase Receipt Item ---------------------------
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
                            set_warehouse_id,
                            company_id,
                            territory_id,
                            name,
                            pr.workflow_state,
                            i.purchase_order,
                            i.item_id,
                            i.item_code,
                            i.item_name,
                            i.uom,
                            --i.conversion_factor,
                            i.stock_uom,
                            i.item_group_id,
                            --pri.brand
                            supplier,
                            --supplier_name,
                            --avg(rate) as rate,
                            i.received_qty,
                            --sum(amount) as amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            ),
--------------------------- Mashup --------------------------------------
mr_with_po_with_pr_cte as (
                              select distinct date(mri.date_created) as material_request_creation_date,
                              mri.warehouse_id,
                              mri.territory_id,
                              mri.name as meterial_request,
                              mri.item_group as item_group_id,
                              mri.item_code,
                              mri.item_name,
                              mri.uom,
                              mri.qty as material_request_qty
                              from material_request_items mri
                              left join purchase_order_items poi on mri.name = poi.material_request and mri.item_name = poi.item_code_id and mri.uom = poi.stock_uom
                              left join purchase_receipt_items pri on poi.id = pri.purchase_order and poi.item_code_id = pri.item_code and poi.stock_uom = pri.stock_uom
                              ),
purchase_order_with_purchase_receipt as (
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
                                        coalesce(poi.item_group, pri.item_group_id) as item_group_id,
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
                  --stock_uom,
                  last_value(item_group_id)over(partition by item_code order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as item_group_id
                  from purchase_order_with_purchase_receipt
                  ),
get_pending_po as (
                    select distinct --company_id,
                    territory_id,
                    item_code,
                    stock_uom,
                    max(purchase_order_creation_date) as pending_purchase_order_creation_date,
                    count(distinct purchase_order_id) as count_of_pending_purchase_order,
                    sum(qty_of_purchase_order) as qty_of_pending_purchase_order
                    from purchase_order_with_purchase_receipt
                    where workflow_state_of_purchase_order in ('PENDING', 'SUBMITTED')
                    and date(purchase_order_creation_date) >= date_sub(current_date, interval 1 month)
                    group by 1,2,3
                    ),
get_completed_po_and_pr as (
                            select *
                            from purchase_order_with_purchase_receipt
                            where workflow_state_of_purchase_receipt  in ('COMPLETED')
                            ),
get_latest_suppliers as (
  select distinct 
  --company_id,
  --warehouse_id,
  territory_id,
  item_code,
  last_value(supplier)over(partition by territory_id, item_code order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_supplier,
  last_value(posting_date_of_purchase_receipt)over(partition by territory_id, item_code order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_posting_date
  from get_completed_po_and_pr
  ),
get_daily_pr_received_qty as (
                            select distinct date(posting_date_of_purchase_receipt) as posting_date_of_purchase_receipt,
                            --company_id,
                            territory_id,
                            --warehouse_id,
                            item_code,
                            stock_uom, 
                            sum(purchase_receipt_received_qty) as qty_received_from_purchase_receipt,
                            from get_completed_po_and_pr
                            group by 1,2,3,4
                            ),
get_po_to_pr_avg_supplier_lead_time as (
      select distinct --company_id,
      --warehouse_id,
      territory_id,
      supplier,
      round(avg(purchase_order_to_purchase_receipt_lead_time)over(partition by territory_id, supplier order by purchase_order_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as      
      calculated_supplier_lead_time
      from get_completed_po_and_pr
      ),
----------------------------------------- Demand - Delivery Notes ---------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at) > date_sub(current_date, interval 2 month)
                --and date(created_at) > '2023-08-05'
                ),
delivery_notes_items as (
                        select dn.delivery_window.delivery_date as scheduled_delivery_date,
                        --date(dn.delivery_date) as delivery_date,
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
----------------- Four Weeks Demand QTY Report -------------------------
four_weeks_demand_qty_report as (
                select distinct demand_plan_start_date as four_week_demand_plan_start_date,
                demand_plan_end_date as four_week_demand_plan_end_date,
                territory_id,
                dp.stock_item_id,
                item_group_id,
                dp.uom,
                dp.weekly_demand_qty
                from `karuru_scheduled_queries.demand_plan` dp
                ),
--------------------------------- Model ------------------------------------------------
stock_balance_with_purchases_report as (
  select distinct osbwt.opening_balance_date,
  osbwt.four_week_demand_plan_start_date,
  osbwt.four_week_demand_plan_end_date,
  osbwt.company_id,
  osbwt.original_territory_id,
  osbwt.new_territory_id,
  osbwt.new_territory_id as territory_id,
  osbwt.warehouse,
  osbwt.item_code,
  osbwt.stock_uom,
  gig.item_group_id,
  gls.latest_supplier,
  gls.latest_posting_date as purchase_receipt_latest_posting_date,
  case
    when gls.latest_posting_date is null then FALSE
  else TRUE end as available_historical_purchase_receipt,
  utsltt.suplier_lead_time,
  safe_cast(gpotpraslt.calculated_supplier_lead_time as int64) as calculated_supplier_lead_time,
  coalesce(utsltt.suplier_lead_time, safe_cast(gpotpraslt.calculated_supplier_lead_time as int64)) + 1 as adjusted_supplier_lead_time,

  osbwt.qty_after_transaction as opening_stock_qty,
  coalesce(gdprrq.qty_received_from_purchase_receipt,0) as purchase_receipt_received_qty,
  --osbwt.qty_after_transaction +  coalesce(gdprrq.qty_received_from_purchase_receipt,0) as adjusted_stock_qty,
  coalesce(gppo.qty_of_pending_purchase_order, 0) as purchase_order_pending_qty,
  --coalesce(gppo.count_of_pending_purchase_order, 0) as count_of_pending_purchase_order,
  gppo.pending_purchase_order_creation_date,
  --date_add(gppo.pending_purchase_order_creation_date, interval )
  from opening_stock_balance_with_territories osbwt
  left join get_item_groups gig on osbwt.item_code = gig.item_code --and osbwt.stock_uom = gig.stock_uom
  left join get_latest_suppliers gls on osbwt.original_territory_id = gls.territory_id and osbwt.item_code = gls.item_code
  left join get_daily_pr_received_qty gdprrq on osbwt.original_territory_id = gdprrq.territory_id and osbwt.item_code = gdprrq.item_code and osbwt.stock_uom = gdprrq.stock_uom and osbwt.opening_balance_date = gdprrq.posting_date_of_purchase_receipt
  left join uploaded_territory_supplier_lead_times_table utsltt on osbwt.original_territory_id = utsltt.territory_id and utsltt.supplier = gls.latest_supplier
  left join get_po_to_pr_avg_supplier_lead_time gpotpraslt on gls.territory_id = gpotpraslt.territory_id and gls.latest_supplier = gpotpraslt.supplier
  left join get_pending_po gppo on osbwt.original_territory_id = gppo.territory_id and osbwt.item_code = gppo.item_code and osbwt.stock_uom = gppo.stock_uom
  ),
add_daily_demand_qty_report as (
  select sbwpr.*,
  dniq.inventory_item_total_qty as total_demand_qty,
  round(safe_divide(dniq.inventory_item_total_qty,adjusted_supplier_lead_time),1)  as daily_demand_qty_from_supplier_lead_time,
  round(fwdqr.weekly_demand_qty,1) as weekly_demand_qty_from_four_wks_run_rate,
  round(fwdqr.weekly_demand_qty / 6,1) as daily_demand_qty_from_four_wks_run_rate,
  coalesce(round(sbwpr.opening_stock_qty / safe_divide(dniq.inventory_item_total_qty,adjusted_supplier_lead_time),0),0) as days_of_stock_cover
  from stock_balance_with_purchases_report sbwpr
  left join dns_items_qty dniq on sbwpr.original_territory_id = dniq.territory_id and sbwpr.item_code = dniq.stock_item_id and sbwpr.stock_uom = dniq.stock_uom and sbwpr.adjusted_supplier_lead_time = dniq.scheduled_delivery_date_index
  left join four_weeks_demand_qty_report fwdqr on sbwpr.original_territory_id = fwdqr.territory_id and sbwpr.item_code = fwdqr.stock_item_id and sbwpr.stock_uom = fwdqr.uom and sbwpr.four_week_demand_plan_start_date = fwdqr.four_week_demand_plan_start_date and sbwpr.four_week_demand_plan_end_date = fwdqr.four_week_demand_plan_end_date
  ),
get_stock_status_report as (
                            select *,
                            date_add(pending_purchase_order_creation_date, interval adjusted_supplier_lead_time day) as expected_date_for_pending_purchae_order,
                              case
                                when (available_historical_purchase_receipt is false) then 'Inter-Warehouse Transfer Stock'
                                when (available_historical_purchase_receipt is true) and (days_of_stock_cover <= 3) then 'Out Of Stock'
                                when (available_historical_purchase_receipt is true) and (days_of_stock_cover > 7) then 'SLOB'
                                when (available_historical_purchase_receipt is true) and (days_of_stock_cover between 3.1 and 7 ) then 'In Stock'
                            else 'UNSET' end as stock_status
                            from add_daily_demand_qty_report
                            )
select *
--distinct stock_status, count(distinct item_code)
--from purchase_order_and_purchase_receipt
from get_stock_status_report
where original_territory_id not in ('Karatina','Nakuru', 'Kyosk HQ', 'Test KE Territory', 'Mtwapa Mombasa', 'Ongata Rongai', 'Kawangware', 'Kisii', 'Meru', 'Eldoret', 'Ruai', 'Athi River', 'Nyeri', 'Juja', 'Thika Rd')
and FORMAT_DATE('%Y%m%d', opening_balance_date) between @DS_START_DATE and @DS_END_DATE
--and opening_balance_date = current_date 
and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
--and territory_id = 'Ruiru'
--and original_territory_id = 'Majengo Mombasa'
--and warehouse = 'Ruiru Main - KDKE'
--and item_code = 'Sunsalt Salt 500G'
--order by item_code, stock_uom, territory_id