------------------- Stock Ledger Entry, Front Mrgins, Material Request, Purchase Order, Purchase Receipt ---------------------
-------------------- v3, Stock Replenishments ---------------
with
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  --SELECT DATE '2024-09-18' as current_start_date,  DATE '2021-09-18' as current_end_date ),

date_vars as (  
              select *,
                date_sub(current_start_date, interval 7 day) as previous_seven_day_start_date,
                date_sub(current_start_date, interval 1 day) as previous_seven_day_end_date,
              from vars
                ),
uploaded_territory_mapping as (
                      select distinct original_territory_id,
                      new_territory_id,
                      warehouse_name,
                      from `karuru_upload_tables.territory_region_mapping` 
                      ),
---------------------------- Upload - Item Group Type -----------------------------------------------
uploaded_item_group_mapping as (
                        SELECT distinct country_code,
                        item_group_id,
                        type
                        FROM `kyosk-prod.karuru_upload_tables.item_group_mapping` 
                        where country_code = 'KE'
                        ),
--------------------------------- Upload - Supplier Lead Times -----------------------------------
uploaded_territory_supplier_lead_times_cte as (
                                                SELECT distinct company_id, 
                                                supplier,
                                                territory_id, 
                                                safe_cast(suplier_lead_time as int64) as suplier_lead_time
                                                FROM `kyosk-prod.karuru_upload_tables.territory_supplier_lead_time` 
                                                ),
--------------------------------- Upload - Skus To Be Deleted -------------------------
uploaded_skus_to_be_disabled as (
                                SELECT distinct company_id, 
                                item_code, 
                                status 
                                FROM `kyosk-prod.karuru_upload_tables.skus_to_be_disabled` 
                                ),
uploaded_skus_to_be_disabled_per_territory as (
                                SELECT distinct warehouse, 
                                item_code, 
                                stock_uom, 
                                status
                                FROM `kyosk-prod.karuru_upload_tables.skus_to_be_disabled_per_territory` 
                                ),
------------ Opening Stock Balances --------------
opening_stock_balance_cte as (
                              select distinct osb.opening_balance_date as opening_stock_balance_date,
                              date_sub(date_trunc(opening_balance_date,week(monday)), interval 4 week) as four_week_demand_plan_start_date,
                              date_sub(date_trunc(opening_balance_date,week(monday)), interval 1 day)  as four_week_demand_plan_end_date,
                              osb.company_id,
                              osb.warehouse,
                              utm.original_territory_id,
                              utm.new_territory_id,
                              osb.item_code,
                              osb.stock_uom,
                              round(sum(osb.qty_after_transaction)) as opening_stock_balance_qty,
                              round(sum(osb.stock_value)) as opening_stock_balance_value
                              FROM `kyosk-prod.karuru_scheduled_queries.opening_stock_balance`  osb
                              left join uploaded_territory_mapping utm on osb.warehouse = utm.warehouse_name
                              where warehouse in ('Eastlands Main - KDKE', 'Embu Main - KDKE', 'Kiambu Main - KDKE', 'Kisumu 1 Main - KDKE', 'Majengo Mombasa Main - KDKE', 'Ruiru Main - KDKE', 'Voi Main - KDKE')
                              --and qty_after_transaction > 0
                              --and opening_balance_date >= date_sub(current_date, interval 1 day)
                              --and opening_balance_date = date_sub(current_date, interval 1 day)
                              and opening_balance_date = current_date
                              and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                              group by 1,2,3,4,5,6,7,8,9
                              ),
----------------------------------------- Demand - Delivery Notes ---------------------
four_weeks_demand_plan_cte as (
                select distinct demand_plan_start_date as four_week_demand_plan_start_date,
                demand_plan_end_date as four_week_demand_plan_end_date,
                territory_id,
                dp.stock_item_id,
                item_group_id,
                dp.uom,
                dp.daily_demand_qty,
                dp.weekly_demand_qty
                from `karuru_scheduled_queries.demand_plan` dp
                ),
------------------------ Scheduled Front Margins -------------------------
front_margins_cte as (
                        SELECT distinct delivery_date,
                        company,
                        territory_id,
                        item_name_of_packed_item, 
                        uom_of_packed_item,                       
                        sum(base_amount) as base_amount
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 1 month)
                        --where delivery_date = date_sub(current_date, interval 1 day)
                        group by 1,2,3,4,5
                        ),
previous_seven_day_front_margins_cte as (
                                      select distinct territory_id,
                                      item_name_of_packed_item as item_code,
                                      uom_of_packed_item as stock_uom,
                                      sum(base_amount) as gmv_vat_incl,
                                      max(delivery_date) as latest_delivery_date
                                      FROM front_margins_cte, date_vars
                                      where delivery_date between previous_seven_day_start_date and previous_seven_day_end_date
                                      --where delivery_date between date_sub(current_date, interval 7 day) and date_sub(current_date, interval 1 day)
                                      group by 1,2,3
                                      ),
-------------------- Material Reqests ---------------------
material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    where date(date_created) >= date_sub(current_date, interval 6 month)
                    and target_warehouse_territory_id not in ('Kyosk HQ', 'Nakuru', 'Karatina', 'Eldoret', 'Ongata Rongai', 'Athi River', 'Kawangware', 'Juja', 'Thika Rd', "Ruai", 'Kisii', 'Meru', 'Mtwapa Mombasa')
                    --and set_warehouse_id not in ('Karatina Receiving Bay - KDKE', 'Eldoret Receiving Bay - KDKE', 'Ongata Rongai Receiving Bay - KDKE', 'Athi River Receiving Bay - KDKE', 'Kawangware Receiving Bay - KDKE')
                    --where date(date_created) between '2024-08-01' and '2024-08-31'
                    and material_request_type = 'PURCHASE'
                    and workflow_state not in ('REJECTED')
                    and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
                    --and name = 'MAT-MR-2023-20063'
                    --and name = 'MAT-MR-2024-15565'
                    --and id in ("MAT-MR-2024-11775")
                    --and name = 'MAT-MR-2024-15325'
                    --and name = 'MAT-MR-2024-15571'
                  ),
material_request_items as (
                            select distinct mr.date_created,
                            --mr.transaction_date,
                            --mr.scheduled_date,

                            mr.company_id,
                            mr.set_warehouse_id,
                            mr.target_warehouse_territory_id,
                            i.warehouse_id,

                            mr.id,
                            mr.name, 
                            mr.workflow_state,
                            mr.status,

                            i.item_group,
                            i.item_id,
                            i.item_code,
                            i.item_name,
                            i.stock_uom,
                            i.conversion_factor,
                            i.uom,

                            i.stock_qty,
                            i.qty,
                            i.ordered_qty,
                            i.received_qty

                            --mri.rate,
                            --mri.amount                                
                            from material_request mr, unnest(items) i
                            where index = 1
                            ),
latest_material_requests_cte as (
                          select distinct target_warehouse_territory_id,
                          warehouse_id,
                          --item_id,
                          item_code,
                          --item_name,
                          last_value(date(date_created))over(partition by warehouse_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_creation_date,
                          last_value(item_group)over(partition by warehouse_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_item_group,
                          from material_request_items
                          ),
pending_material_requests_cte as (
                              select distinct target_warehouse_territory_id,
                              item_code,
                              stock_uom,

                              count(distinct(case when status = 'DRAFT' then id else null end)) as mr_count_in_draft_status,
                              count(distinct(case when status = 'ORDERED' then id else null end)) as mr_count_in_ordered_status,
                              sum(case when status = 'DRAFT' then qty else 0 end) as qty_in_draft_status,
                              --sum(case when status = 'PARTIALLY_ORDERED' then ordered_qty else 0 end) as ordered_qty_in_partially_ordered_status,
                              sum(case when status = 'ORDERED' then ordered_qty else 0 end) as ordered_qty_in_ordered_status,
                              string_agg(distinct status, "/" order by status) as status,
                              --sum(ordered_qty) as ordered_qty,
                              max(case when status in ('DRAFT', 'ORDERED') then date(date_created) else null end) as pending_mr_latest_creation_date,
                              --max(date(date_created)) as max_creation_date
                              from material_request_items, date_vars
                              where status in ('DRAFT', 'ORDERED', 'PARTIALLY_ORDERED')
                              and date(date_created) <= current_start_date
                              group by 1,2,3
                              ),
------------------------------- Purchase Order Item ----------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where date(creation) >= date_sub(current_date, interval 6 month)
                    ),
purchase_order_items as (
                          select distinct creation,
                          po.company,
                          po.territory,
                          po.warehouse_territory,
                          i.warehouse_id,
                          --purchase_order_date,

                          po.workflow_state,
                          --fulfillment_date,

                          i.material_request,
                          po.id,
                          po.purchase_order_no,
                          
                          i.item_code_id,
                          i.item_name,
                          i.stock_uom,
                          i.qty,
                          i.stock_qty,
                          i.item_group,

                          po.supplier,
                          po.supplier_name,
                          from purchase_order po, unnest(items) i
                          where index =1
                          ),
latest_purchase_order_cte as (
                          select distinct --company,
                          --warehouse_id,
                          --warehouse_territory,
                          territory,
                          item_code_id,
                          last_value(date(creation))over(partition by territory, item_code_id order by creation asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_creation_date,
                          last_value(supplier)over(partition by territory, item_code_id order by creation asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_supplier,
                          from purchase_order_items
                          ),
---------------------------- Purchase Receipt Items ----------------------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              where date(date_created) >= date_sub(current_date, interval 12 month)
              and territory_id not in ('Test UG Territory', 'Test KE Territory', 'Kawangware', 'Juja', 'Ongata Rongai', 'Kisii', 'Nakuru', 'Athi River', 'Karatina', 'Eldoret', 'Thika Rd', 'Mtwapa Mombasa', 'Ruai')
              and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
              ),
purchase_receipt_items_cte as (
                            select distinct date_created,
                            cast(posting_date as date) as posting_date,
                            --posting_time,
                            --company_id,
                            pr.set_warehouse_id,
                            pr.territory_id,

                            pr.id,
                            pr.name,
                            i.material_request_id,
                            i.purchase_order,

                            pr.workflow_state,
                            --i.item_id,
                            i.item_code,
                            i.item_name,
                            i.uom,
                            --i.conversion_factor,
                            i.stock_uom,
                            i.item_group_id,
                            --pri.brand

                            pr.supplier,
                            --supplier_name,

                            i.received_qty,
                            i.amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            ),
latest_purchase_receipt_cte as (
                          select distinct --set_warehouse_id,
                          territory_id,
                          item_code,
                          last_value(date(date_created))over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_creation_date,
                          last_value(posting_date)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_posting_date,
                          last_value(supplier)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_supplier,
                          last_value(item_group_id)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_item_group_id,
                          from purchase_receipt_items_cte
                          ),
received_purchase_receipt_cte as (
                                  select distinct posting_date,
                                  set_warehouse_id,
                                  territory_id,
                                  item_code,
                                  stock_uom,
                                  sum(received_qty) as received_qty
                                  from purchase_receipt_items_cte
                                  group by 1,2,3,4,5
                                  ),
------------------------------ Mashup -------------------
stock_replenishment_cte as (
                              select osb.*,
                              round(safe_divide(osb.opening_stock_balance_qty, d.weekly_demand_qty) * 6) as opening_stock_cover_days,
                              coalesce(round(d.daily_demand_qty), 0) as daily_demand_qty,
                              coalesce(round(d.weekly_demand_qty),0) as weekly_demand_qty,
                              psdfm.latest_delivery_date,
                              psdfm.gmv_vat_incl as last_seven_day_gmv_vat_incl,

                              coalesce(lmr.latest_mr_item_group, lpr.latest_pr_item_group_id, 'UNSET') as item_group_id,
                              coalesce(lpr.latest_pr_supplier, lpo.latest_po_supplier) as supplier,

                              lmr.latest_mr_creation_date,
                              case
                                when (lpr.latest_pr_posting_date is null) then 'No Recent Purchase Receipt'
                              else 'Has Recent Purchase Receipt' end as check_recent_purchase_receipt,
                              case
                                when (ustbdpt.status is not null) then 'To Be Disabled' 
                                when (ustbdpt.status is null) and (osb.opening_stock_balance_qty = 0) then 'Zero Stock Balance'
                                when (ustbdpt.status is null) and (osb.opening_stock_balance_qty > 0) then 'With Stock Balance Qty'
                              else 'Active' end as check_opening_stock_balance,
                              lmr.latest_mr_item_group,

                              pmr.mr_count_in_draft_status,
                              pmr.mr_count_in_ordered_status,
                              pmr.qty_in_draft_status as mr_qty_in_draft_status,
                              --pmr.ordered_qty_in_partially_ordered_status as mr_ordered_qty_in_partially_ordered_status,
                              pmr.ordered_qty_in_ordered_status as mr_ordered_qty_in_ordered_status,
                              pmr.status as pending_mr_statuses,
                              --pmr.max_creation_date as pending_mr_max_creation_date,
                              pmr.pending_mr_latest_creation_date,


                              lpo.latest_po_creation_date,
                              lpo.latest_po_supplier,

                              coalesce(rpr.received_qty, 0) as pr_received_qty,

                              lpr.latest_pr_creation_date,
                              lpr.latest_pr_posting_date,
                              lpr.latest_pr_item_group_id,
                              lpr.latest_pr_supplier,
                              case
                                when (lmr.latest_mr_creation_date is not null) then date_diff(lpr.latest_pr_creation_date, lpo.latest_po_creation_date, day) 
                              else null end as calculated_supplier_lead_time
                              from opening_stock_balance_cte osb
                              --left join uploaded_skus_to_be_disabled ustbd on osb.company_id = ustbd.company_id and osb.item_code = ustbd.item_code
                              left join uploaded_skus_to_be_disabled_per_territory ustbdpt on osb.warehouse = ustbdpt.warehouse and osb.item_code = ustbdpt.item_code and osb.stock_uom = ustbdpt.stock_uom
                              left join four_weeks_demand_plan_cte d on osb.item_code = d.stock_item_id and osb.stock_uom = d.uom and osb.original_territory_id = d.territory_id and osb.four_week_demand_plan_start_date = d.four_week_demand_plan_start_date
                              and osb.four_week_demand_plan_end_date = d.four_week_demand_plan_end_date
                              left join previous_seven_day_front_margins_cte psdfm on osb.original_territory_id = psdfm.territory_id and osb.item_code = psdfm.item_code and osb.stock_uom = psdfm.stock_uom

                              left join latest_material_requests_cte lmr on osb.original_territory_id = lmr.target_warehouse_territory_id and osb.item_code = lmr.item_code
                              left join pending_material_requests_cte pmr on osb.original_territory_id = pmr.target_warehouse_territory_id and osb.item_code = pmr.item_code and osb.stock_uom = pmr.stock_uom

                              left join latest_purchase_order_cte lpo on osb.original_territory_id = lpo.territory and osb.item_code = lpo.item_code_id
                              left join latest_purchase_receipt_cte lpr on osb.original_territory_id = lpr.territory_id and osb.item_code = lpr.item_code
                              left join received_purchase_receipt_cte rpr on osb.opening_stock_balance_date = rpr.posting_date and osb.original_territory_id = rpr.territory_id and osb.item_code = rpr.item_code and osb.stock_uom = rpr.stock_uom
                              ),
updated_stock_replenishment_cte as (
                                    select distinct sr.opening_stock_balance_date,
                                    --sr.four_week_demand_plan_start_date,
                                    --sr.four_week_demand_plan_end_date,
                                    sr.company_id,
                                    sr.warehouse,
                                    sr.original_territory_id,
                                    sr.new_territory_id,

                                    sr.supplier,
                                    utslt.suplier_lead_time as purchasing_team_supplier_lead_time,
                                    sr.calculated_supplier_lead_time,
                                    coalesce(utslt.suplier_lead_time, sr.calculated_supplier_lead_time) as supplier_lead_time,
                                    1 + coalesce(utslt.suplier_lead_time, sr.calculated_supplier_lead_time) as adjusted_supplier_lead_time,

                                    --sr.sku_active_status,
                                    sr.check_opening_stock_balance,
                                    coalesce(uigm.type, 'UNSET') as item_group_type,
                                    sr.item_group_id,
                                    sr.item_code,
                                    sr.stock_uom,

                                    sr.opening_stock_balance_qty,
                                    sr.opening_stock_balance_value,
                                    sr.daily_demand_qty,
                                    sr.weekly_demand_qty,
                                    sr.opening_stock_cover_days,

                                    round(safe_divide(sr.opening_stock_balance_qty, sr.opening_stock_cover_days) * 4) as minimum_stock_qty,
                                    round(safe_divide(sr.opening_stock_balance_value, sr.opening_stock_cover_days) * 4) as minimum_stock_value,

                                    round(safe_divide(sr.opening_stock_balance_qty, sr.opening_stock_cover_days) * 5) as re_order_point_stock_qty,
                                    round(safe_divide(sr.opening_stock_balance_value, sr.opening_stock_cover_days) * 5) as re_order_point_stock_value,

                                    round(safe_divide(sr.opening_stock_balance_qty, sr.opening_stock_cover_days) * 7) as maximum_7_day_stock_qty,
                                    round(safe_divide(sr.opening_stock_balance_value, sr.opening_stock_cover_days) * 7) as maximum_7_day_stock_value,

                                    sr.latest_delivery_date,
                                    sr.last_seven_day_gmv_vat_incl,

                                    sr.pending_mr_latest_creation_date,
                                    sr.mr_count_in_draft_status,
                                    sr.mr_count_in_ordered_status,
                                    sr.mr_qty_in_draft_status,
                                    sr.mr_ordered_qty_in_ordered_status,

                                    sr.pr_received_qty,
                                    sr.latest_pr_creation_date,
                                    sr.latest_pr_posting_date,

                                    from stock_replenishment_cte sr
                                    left join uploaded_territory_supplier_lead_times_cte utslt on sr.original_territory_id = utslt.territory_id and sr.supplier = utslt.supplier
                                    left join uploaded_item_group_mapping uigm on sr.item_group_id = uigm.item_group_id
                                    ),
stock_replenishment_with_stock_status_cte as (
                                            select usr.*,
                                            case
                                              when check_opening_stock_balance in ('To Be Disabled') then 'To Be Disabled' 
                                              when check_opening_stock_balance in ('Zero Stock Balance') and (usr.weekly_demand_qty = 0) then 'Zero Stock Balance'
                                              when check_opening_stock_balance in ('Zero Stock Balance') and (usr.weekly_demand_qty > 0) then 'Out Of Stock'
                                              --when sku_active_status in ('No Recent Purchase Receipt') then 'No Recent Purchase Receipt'
                                              when check_opening_stock_balance in ('With Stock Balance Qty') and (opening_stock_cover_days between 0 and 3) then 'Out Of Stock'
                                              when check_opening_stock_balance in ('With Stock Balance Qty') and (opening_stock_cover_days between 4 and 7) then '4 to 7-Day Available Stock'
                                              when check_opening_stock_balance in ('With Stock Balance Qty') and (weekly_demand_qty = 0) then 'SLOB'
                                              when check_opening_stock_balance in ('With Stock Balance Qty') and (opening_stock_cover_days > 7) and (opening_stock_balance_qty > maximum_7_day_stock_qty) then 'SLOB'
                                            else 'UNSET' end as stock_position_status,
                                            from updated_stock_replenishment_cte usr
                                            ),
stock_replenishment_with_recommendation_cte as (
                                                select srwss.*,
                                                  case
                                                    when stock_position_status in ('To Be Disabled') then 'To Be Disabled'
                                                    when stock_position_status in ('Out Of Stock') and (mr_qty_in_draft_status > 0) then 'Action On Draft Material Request'
                                                    when stock_position_status in ('Out Of Stock') and (mr_ordered_qty_in_ordered_status > 0) then 'Yet To Receive Purchase Orders'
                                                    when stock_position_status in ('Out Of Stock') and (mr_qty_in_draft_status = 0) and (mr_ordered_qty_in_ordered_status = 0) then 'Create A Material Request'
                                                  else 'UNSET' end as recommendation
                                                from stock_replenishment_with_stock_status_cte srwss
                                                )
--select * from updated_stock_replenishment_cte
select * from stock_replenishment_with_recommendation_cte --WHERE original_territory_id = 'Kiambu' --and item_code = 'Prestige Original Margarine 250g'
where FORMAT_DATE('%Y%m%d', opening_stock_balance_date) between @DS_START_DATE and @DS_END_DATE
--where item_code = 'Mt. Kenya Milk ESL 500ML - 18 PC'
--order by opening_stock_balance_date, warehouse, item_code