------------------- Stock Ledger Entry, Front Mrgins, Material Request, Purchase Order, Purchase Receipt ---------------------
-------------------- v3, Stock Replenishments ---------------
with
/*
vars AS (
  --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE)  as current_end_date ),
  SELECT DATE '2024-10-22' as current_start_date,  DATE '2021-10-22' as current_end_date ),

date_vars as (  
              select *,
                date_sub(current_start_date, interval 7 day) as previous_seven_day_start_date,
                date_sub(current_start_date, interval 1 day) as previous_seven_day_end_date,
              from vars
                ),
*/
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
                            round(osb.qty_after_transaction) as opening_stock_balance_qty,
                            round(osb.valuation_rate) as opening_stock_balance_valuation_rate,
                            round(osb.stock_value) as opening_stock_balance_value,
                            osb.avg_age as opening_stock_balance_avg_age,
                            --round(sum(osb.qty_after_transaction)) as opening_stock_balance_qty,
                            --round(sum(osb.stock_value)) as opening_stock_balance_value
                            FROM `kyosk-prod.karuru_scheduled_queries.opening_stock_balance`  osb
                            left join uploaded_territory_mapping utm on osb.warehouse = utm.warehouse_name
                            where warehouse in ('Eastlands Main - KDKE', 'Embu Main - KDKE', 'Kiambu Main - KDKE', 'Kisumu 1 Main - KDKE', 'Majengo Mombasa Main - KDKE', 'Ruiru Main - KDKE', 'Voi Main - KDKE')
                            and opening_balance_date = current_date
                            --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --group by 1,2,3,4,5,6,7,8,9
                            ),
----------------------------------------- Scheduled Query - Demand Plan ---------------------
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
------------------------ Scheduled Query - Front Margins -------------------------
scheduled_query_front_margins_report_cte as (
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
                                      FROM scheduled_query_front_margins_report_cte--, date_vars
                                      --where delivery_date between previous_seven_day_start_date and previous_seven_day_end_date
                                      --where delivery_date between date_sub(current_date, interval 7 day) and date_sub(current_date, interval 1 day)
                                      group by 1,2,3
                                      ),
-------------------- Material Reqests ---------------------
material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    --where date(date_created) >= date_sub(current_date, interval 12 month)
                    where date(date_created) between date_sub(date_trunc(current_date, month), interval 12 month) and date(current_date)
                    and material_request_type = 'PURCHASE'
                    and workflow_state not in ('REJECTED')
                    and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
                  ),
material_request_items_cte as (
                            select distinct mr.date_created,

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
------------------------------- Purchase Order Item ----------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where date(creation) between date_sub(date_trunc(current_date, month), interval 12 month) and date(current_date)
                    ),
purchase_order_items_cte as (
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
---------------------------- Purchase Receipt Items ----------------------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              --where date(date_created) >= date_sub(current_date, interval 12 month)
              where date(date_created) between date_sub(date_trunc(current_date, month), interval 12 month) and date(current_date)
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
------------------------------------------- MR, PO and PR Mashup ----------------------------
mr_po_and_pr_cte as (
            select distinct mri.date_created as mr_creation_datetime,
            date(mri.date_created) as mr_creation_date,
            mri.company_id as company_id,
            mri.warehouse_id as warehouse_id,
            mri.warehouse_id as mr_warehouse_id,
            mri.set_warehouse_id as mr_set_warehouse_id,
            mri.target_warehouse_territory_id as mr_target_warehouse_territory_id,
            mri.target_warehouse_territory_id as territory_id,

            mri.id as material_request_id,
            mri.name as material_request,
            mri.workflow_state as mr_workflow_state,
            mri.status as mr_status,

            case
              when (mri.status = 'DRAFT') and (poi.workflow_state is null) and (pri.workflow_state is null) then 'MR In Draft; PO Is Null; PR Is Null'
              when (mri.status = 'DRAFT') and (poi.workflow_state = 'CANCELLED') and (pri.workflow_state is null) then 'MR In Draft; PO Cancelled; PR Is Null'
              when (mri.status = 'DRAFT') and (poi.workflow_state = 'PENDING') and (pri.workflow_state is null) then 'MR In Draft; PO In Pending; PR Is Null'
              when (mri.status = 'DRAFT') and (poi.workflow_state = 'SUBMITTED') and (pri.workflow_state is null) then 'MR In Draft; PO In Submitted; PR Is Null'
              when (mri.status = 'DRAFT') and (poi.workflow_state = 'REJECTED') and (pri.workflow_state is null) then 'MR In Draft; PO In Rejected; PR Is Null'

              when (mri.status = 'ORDERED') and (poi.workflow_state is null) and (pri.workflow_state is null) then 'MR In Ordered; PO Is Null; PR Is Null'
              when (mri.status = 'ORDERED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state is null) then 'MR In Ordered; PO In Approved; PR Is Null'
              when (mri.status = 'ORDERED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state  = 'COMPLETED') then 'MR In Ordered; PO Approved; PR In Completed'
              when (mri.status = 'ORDERED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'SUBMITTED') then 'MR In Ordered; PO Approved; PR In Submitted'

              when (mri.status = 'PARTIALLY_ORDERED') and (poi.workflow_state is null) and (pri.workflow_state is null) then 'MR In Partially Ordered; PO Is Null; PR Is Null'
              when (mri.status = 'PARTIALLY_ORDERED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state is null) then 'MR In Partially Ordered; PO In Approved; PR Is Null'
              when (mri.status = 'PARTIALLY_ORDERED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'COMPLETED') then 'MR In Partially Ordered; PO In Approved; PR In Completed'
              when (mri.status = 'PARTIALLY_ORDERED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'REJECTED') then 'MR In Partially Ordered; PO In Approved; PR In Rejected'

              when (mri.status = 'PARTIALLY_RECEIVED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state is null) then 'MR In Partially Received; PO In Approved; PR Is Null'
              when (mri.status = 'PARTIALLY_RECEIVED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'COMPLETED') then 'MR In Partially Received; PO In Approved; PR In Completed'
              when (mri.status = 'PARTIALLY_RECEIVED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'REJECTED') then 'MR In Partially Received; PO In Approved; PR In Rejected'

              when (mri.status = 'RECEIVED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state is null) then 'MR In Received; PO In Approved; PR Is Null'
              when (mri.status = 'RECEIVED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'COMPLETED') then 'MR In Received; PO In Approved; PR In Completed'
              when (mri.status = 'RECEIVED') and (poi.workflow_state = 'APPROVED') and (pri.workflow_state = 'REJECTED') then 'MR In Received; PO In Approved; PR In Rejected'
            else 'UNSET' end as statuses_info,

            mri.item_group as mr_item_group,
            --mri.item_code as mr_item_code,
            --mri.stock_uom as mr_stock_uom,
            mri.item_code as item_code,
            mri.stock_uom as stock_uom,
            mri.conversion_factor as mr_conversion_factor,
            --mri.uom as mr_uom,
            mri.uom as uom,

            mri.stock_qty as mr_stock_qty,
            mri.qty as mr_qty,
            mri.received_qty as mr_received_qty,

            poi.creation as po_creation_datetime,
            date(poi.creation) as po_creation_date,
            poi.warehouse_id as po_warehouse_id,
            poi.territory as po_territory,
            poi.id as purchase_order_id,
            poi.purchase_order_no,
            poi.workflow_state as po_workflow_state,
            poi.supplier as po_supplier,

            pri.date_created as pr_creation_datetime,
            date(pri.date_created) as pr_creation_date,
            date(pri.posting_date) as pr_posting_date,
            pri.id as purchase_receipt_id,
            pri.workflow_state as pr_workflow_state,
            pri.received_qty as pr_received_qty,
            pri.supplier as pr_supplier,
            pri.item_group_id as pr_item_group_id,

            from material_request_items_cte mri
            left join purchase_order_items_cte poi on mri.id = poi.material_request and mri.item_code = poi.item_code_id and mri.stock_uom = poi.stock_uom
            left join purchase_receipt_items_cte pri on poi.id = pri.purchase_order and poi.item_code_id = pri.item_code and poi.stock_uom = pri.stock_uom
            where mri.warehouse_id not in ('Kyosk HQ Receiving - KDKE', 'Test KE Receiving Bay - KDKE', 'Test KE Main - KDKE', 'Nakuru Receiving Bay - KDKE', 'Ongata Rongai Receiving Bay - KDKE', 
            'Kisii Receiving bay - KDKE', 'Kawangware Receiving Bay - KDKE', 'Eldoret Receiving Bay - KDKE', 'Athi River Receiving Bay - KDKE', 'Test UG Receiving By - KDUG', 'Test Fresh TZ Receiving Bay - KDTZ',
            'Ruai Receiving Bay - KDKE', 'Themi Receiving Bay - KDTZ', 'Mukono Receiving Bay - KDUG', 'Juja Receiving Bay - KDKE', 'Karatina Receiving Bay - KDKE', 'Meru Receiving Bay - KDKE', 
            'Thika Rd Receiving Bay - KDKE', 'Benin- Sapele Receiving Bay - KDNG', 'Ilorin Main Warehouse - KDNG', 'Ilorin Receiving Bay - KDNG', 'Kano-Sabongari Receiving Bay - KDNG', 
            'Kano-Zoo Receiving Bay - KDNG', 'PortHarcourt-Obiakpor Receiving Bay - KDNG', 'Vingunguti Receiving Bay - KDTZ', 'Okota Receiving Bay - KDNG', 'Surulere Receiving Bay - KDNG',
            'Mtwapa Mombasa Receiving Bay - KDKE')
            ),
--------------------------- Pending MR, PO and PR -----------------------------------------
pending_mr_po_and_pr_cte as (
                            select distinct mr_creation_date,
                            po_creation_date,
                            company_id,
                            warehouse_id,
                            territory_id,
                            statuses_info,
                            material_request_id,
                            purchase_order_id,
                            item_code,
                            stock_uom,
                            mr_stock_qty,
                            mr_qty,
                            from mr_po_and_pr_cte
                            where statuses_info in ('MR In Draft; PO In Pending; PR Is Null', 'MR In Draft; PO Is Null; PR Is Null', 'MR In Received; PO In Approved; PR Is Null')
                            ),
pending_mr_po_and_pr_agg_cte as (
                                select distinct company_id,
                                warehouse_id,
                                territory_id,
                                item_code,
                                stock_uom,
                                sum(case when statuses_info = 'MR In Draft; PO In Pending; PR Is Null' then mr_stock_qty else 0 end) as mr_stock_qty_in_draft_with_pending_po,
                                sum(case when statuses_info = 'MR In Draft; PO Is Null; PR Is Null' then mr_stock_qty else 0 end) as mr_stock_qty_in_draft_with_null_po,
                                sum(case when statuses_info = 'MR In Ordered; PO In Approved; PR Is Null' then mr_stock_qty else 0 end) as  mr_stock_qty_in_ordered_with_approved_po,
                                sum(case when statuses_info = 'MR In Received; PO In Approved; PR Is Null' then mr_stock_qty else 0 end) as  mr_stock_qty_in_received_with_approved_po,
                                max(mr_creation_date) as pending_mr_max_creation_date
                                from pending_mr_po_and_pr_cte
                                group by 1,2,3,4,5
                                ), 
------------------- Latest MR, PO and PR ---------------------------------
latest_sku_details_by_warehouse_cte as (
                select distinct --company_id,
                --warehouse_id,
                territory_id,
                item_code,
                stock_uom,
                last_value(mr_creation_date IGNORE NULLS)over(partition by warehouse_id, item_code order by mr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_mr_creation_date,
                last_value(mr_item_group IGNORE NULLS)over(partition by warehouse_id, item_code order by mr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_mr_warehouse_item_group,

                last_value(date(po_creation_date) IGNORE NULLS)over(partition by warehouse_id, item_code order by po_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_po_creation_date,
                last_value(po_supplier IGNORE NULLS)over(partition by warehouse_id, item_code order by po_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_po_supplier,

                last_value(pr_posting_date IGNORE NULLS)over(partition by warehouse_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_pr_creation_date,
                last_value(pr_posting_date IGNORE NULLS)over(partition by warehouse_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_pr_posting_date,
                last_value(pr_supplier IGNORE NULLS)over(partition by territory_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_pr_supplier,
                from mr_po_and_pr_cte
                      ),
latest_sku_details_by_company_cte as (
                select distinct company_id,
                item_code,
                stock_uom,
                last_value(mr_item_group IGNORE NULLS)over(partition by company_id, item_code order by mr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_item_group,
                last_value(pr_item_group_id IGNORE NULLS)over(partition by company_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as latest_pr_item_group_id,
                from mr_po_and_pr_cte
                      ),
------------------------------ Mashup -------------------
opening_stock_with_purchase_history_cte as (
                      select distinct current_datetime() as scheduled_query_creation_datetime,
                      --'rodgerso65@gmail.com' as scheduled_query_created_by,
                      osb.opening_stock_balance_date,
                      psdfm.latest_delivery_date,
                      osb.four_week_demand_plan_start_date,
                      osb.four_week_demand_plan_end_date,
                      
                      osb.company_id,
                      osb.warehouse,
                      osb.original_territory_id,
                      osb.new_territory_id,

                      coalesce(lwp.latest_pr_supplier, lwp.latest_po_supplier, 'UNSET') as supplier,
                      coalesce(lwp.latest_mr_warehouse_item_group, lsdbc.latest_mr_item_group,lsdbc.latest_pr_item_group_id, 'UNSET') as item_group_id,
                      osb.item_code,
                      osb.stock_uom,

                      ------------ daily & weekly demand qty --------------
                      coalesce(round(d.daily_demand_qty,0), 0) as daily_demand_qty,
                      coalesce(round(d.weekly_demand_qty,0),0) as weekly_demand_qty,

                      osb.opening_stock_balance_qty,
                      case
                        when (rpr.received_qty is null) then 'NO'
                      else 'YES' end as check_pr_received_qty,
                      coalesce(rpr.received_qty, 0) as pr_received_qty,
                      round(osb.opening_stock_balance_qty + coalesce(rpr.received_qty, 0)) as adjusted_opening_balance_qty,

                      osb.opening_stock_balance_valuation_rate,
                      osb.opening_stock_balance_value,
                      cast(round(safe_divide(osb.opening_stock_balance_qty, d.weekly_demand_qty) * 6) as int64) as opening_stock_cover_days,
                      cast(round(safe_divide((osb.opening_stock_balance_qty + coalesce(rpr.received_qty,0)), d.weekly_demand_qty) * 6) as int64) as adjusted_opening_stock_cover_days,
                      osb.opening_stock_balance_avg_age,
                      
                      psdfm.gmv_vat_incl as last_seven_day_gmv_vat_incl,

                      case
                        when (ustbdpt.status is not null) then 'To Be Disabled' 
                        when (ustbdpt.status is null) and (osb.opening_stock_balance_qty = 0) then 'Zero Stock Balance'
                        when (ustbdpt.status is null) and (osb.opening_stock_balance_qty > 0) then 'With Stock Balance Qty'
                      else 'Active' end as check_opening_stock_balance,

                      case
                        when (ustbdpt.status is not null) then 'To Be Disabled' 
                        when (ustbdpt.status is null) and (lwp.latest_pr_posting_date is null) then 'No Latest PR'
                      else 'Has Latest PR' end as check_latest_pr,

                      case
                        when (ustbdpt.status is not null) then 'To Be Disabled' 
                        when (ustbdpt.status is null) and (d.weekly_demand_qty > 0) then 'Has Weekly Demand'
                        when (ustbdpt.status is null) and (d.weekly_demand_qty is null) then 'No Weekly Demand'
                      else 'UNSET' end as check_weekly_demand,
                      /*case
                        when (pmr.mr_count_in_draft_status is null) then 'NO'
                      else 'YES' end as check_mr_in_draft_status,
                      case
                        when (mr_count_in_ordered_status is null) then 'NO'
                      else 'YES' end as check_mr_in_ordered_status,*/

                      
                      case
                        when (lwp.latest_mr_creation_date is not null) then date_diff(lwp.latest_pr_creation_date, lwp.latest_po_creation_date, day) 
                      else null end as calculated_supplier_lead_time,
                      lwp.latest_mr_creation_date,
                      lwp.latest_mr_warehouse_item_group,
                      lsdbc.latest_mr_item_group,

                      lwp.latest_po_creation_date,
                      lwp.latest_po_supplier,

                      lwp.latest_pr_creation_date,
                      lwp.latest_pr_posting_date,
                      lsdbc.latest_pr_item_group_id,
                      lwp.latest_pr_supplier,

                      coalesce(p.mr_stock_qty_in_draft_with_pending_po,0) as mr_stock_qty_in_draft_with_pending_po,
                      coalesce(p.mr_stock_qty_in_draft_with_null_po, 0) as mr_stock_qty_in_draft_with_null_po,
                      coalesce(p.mr_stock_qty_in_ordered_with_approved_po, 0) as mr_stock_qty_in_ordered_with_approved_po,
                      coalesce(p.mr_stock_qty_in_received_with_approved_po, 0) as mr_stock_qty_in_received_with_approved_po
                      from opening_stock_balance_cte osb
                      left join latest_sku_details_by_warehouse_cte lwp on osb.original_territory_id = lwp.territory_id and osb.item_code = lwp.item_code and osb.stock_uom = lwp.stock_uom
                      left join latest_sku_details_by_company_cte lsdbc on osb.company_id = lsdbc.company_id and osb.item_code = lsdbc.item_code
                      left join uploaded_skus_to_be_disabled_per_territory ustbdpt on osb.warehouse = ustbdpt.warehouse and osb.item_code = ustbdpt.item_code and osb.stock_uom = ustbdpt.stock_uom
                      left join four_weeks_demand_plan_cte d on (osb.item_code = d.stock_item_id) and (osb.stock_uom = d.uom) and (osb.original_territory_id = d.territory_id) and 
                      (osb.four_week_demand_plan_start_date = d.four_week_demand_plan_start_date) and osb.four_week_demand_plan_end_date = d.four_week_demand_plan_end_date
                      left join previous_seven_day_front_margins_cte psdfm on osb.original_territory_id = psdfm.territory_id and osb.item_code = psdfm.item_code and osb.stock_uom = psdfm.stock_uom
                      left join received_purchase_receipt_cte rpr on (osb.opening_stock_balance_date = rpr.posting_date) and (osb.original_territory_id = rpr.territory_id) and 
                      (osb.item_code = rpr.item_code and osb.stock_uom = rpr.stock_uom)
                      left join pending_mr_po_and_pr_agg_cte p on (osb.original_territory_id = p.territory_id) and (osb.item_code = p.item_code) and (osb.stock_uom = p.stock_uom)
                      ),
get_re_order_details_cte as (
                              select distinct sr.scheduled_query_creation_datetime,
                              sr.opening_stock_balance_date,
                              sr.latest_delivery_date,
                              sr.four_week_demand_plan_start_date,
                              sr.four_week_demand_plan_end_date,

                              sr.company_id,
                              sr.warehouse,
                              sr.original_territory_id,
                              sr.new_territory_id,

                              coalesce(uigm.type, 'UNSET') as item_group_type,
                              sr.item_group_id,
                              sr.item_code,
                              sr.stock_uom,

                              sr.last_seven_day_gmv_vat_incl,
                              sr.check_weekly_demand,
                              sr.daily_demand_qty,
                              sr.weekly_demand_qty,

                              sr.check_opening_stock_balance,
                              sr.opening_stock_balance_qty,
                              sr.check_pr_received_qty,
                              sr.pr_received_qty,
                              sr.adjusted_opening_balance_qty,

                              sr.opening_stock_balance_valuation_rate,
                              sr.opening_stock_balance_value,
                              sr.opening_stock_cover_days,
                              sr.adjusted_opening_stock_cover_days,
                              sr.opening_stock_balance_avg_age,

                              round(safe_divide(sr.opening_stock_balance_qty, sr.opening_stock_cover_days) * 4) as minimum_stock_qty,
                              round(safe_divide(sr.opening_stock_balance_value, sr.opening_stock_cover_days) * 4) as minimum_stock_value,

                              round(safe_divide(sr.opening_stock_balance_qty, sr.opening_stock_cover_days) * 5) as re_order_point_stock_qty,
                              round(safe_divide(sr.opening_stock_balance_value, sr.opening_stock_cover_days) * 5) as re_order_point_stock_value,

                              round(safe_divide(sr.opening_stock_balance_qty, sr.opening_stock_cover_days) * 7) as inventory_holding_for_7_day_demand,
                              round(safe_divide(sr.opening_stock_balance_qty, sr.adjusted_opening_stock_cover_days) * 7) as adjusted_inventory_holding_for_7_day_demand,

                              round(safe_divide(sr.opening_stock_balance_value, sr.opening_stock_cover_days) * 7) as maximum_7_day_stock_value,

                              --round((safe_divide(sr.opening_stock_balance_qty, sr.adjusted_opening_stock_cover_days) * 7) - (sr.adjusted_opening_balance_qty),0) as stock_deficit,

                              sr.mr_stock_qty_in_draft_with_pending_po,
                              sr.mr_stock_qty_in_draft_with_null_po,
                              sr.mr_stock_qty_in_ordered_with_approved_po,
                              sr.mr_stock_qty_in_received_with_approved_po,

                              --sr.latest_pr_supplier,
                              sr.supplier,
                              --utslt.suplier_lead_time as purchasing_team_supplier_lead_time,
                              --coalesce(utslt.suplier_lead_time, sr.calculated_supplier_lead_time) as supplier_lead_time,
                              --1 + coalesce(utslt.suplier_lead_time, sr.calculated_supplier_lead_time) as adjusted_supplier_lead_time,

                              --sr.latest_po_creation_date,

                              --sr.latest_po_supplier,

                              sr.check_latest_pr,
                              --sr.latest_pr_creation_date,
                              sr.latest_pr_posting_date,

                              --sr.latest_mr_warehouse_item_group,
                              --sr.latest_pr_item_group_id,

                              from opening_stock_with_purchase_history_cte sr
                              left join uploaded_territory_supplier_lead_times_cte utslt on sr.original_territory_id = utslt.territory_id and sr.supplier = utslt.supplier
                              left join uploaded_item_group_mapping uigm on sr.item_group_id = uigm.item_group_id
                              ),
get_stock_position_status_cte as (
                        select usr.*,

                        round(usr.adjusted_opening_stock_cover_days - usr.adjusted_opening_balance_qty) as adjusted_stock_deficit,
                        
                        case
                          when (check_opening_stock_balance = 'To Be Disabled') and (check_latest_pr = 'To Be Disabled') and (check_weekly_demand = 'To Be Disabled') then 'To Be Disabled' 

                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'No Weekly Demand') then 'To Be Activated'
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'No Weekly Demand') then 'Dead Stock'

                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days between 0 and 3) then 'Out Of Stock'
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days between 4 and 7) then '4-7 Days Stock'
                          ---------------------- SLOB ----------------------
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days > 7) and (opening_stock_balance_avg_age > 7) then 'SLOB'
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days > 7) and (opening_stock_balance_avg_age > 7) then 'SLOB'
                          
                          ---------------------- New Listing ----------------------
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days > 7) and (opening_stock_balance_avg_age <= 7) then 'New Listing'
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days > 7) and (opening_stock_balance_avg_age <= 7) then 'New Listing'


                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days between 0 and 3) then 'Out Of Stock'
                          when (check_opening_stock_balance = 'With Stock Balance Qty') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'Has Weekly Demand') 
                            and (opening_stock_cover_days between 4 and 7) then '4-7 Days Stock'

                          ----------------- 'Consider Disabling' - no weekly demand ---------------------- 
                          when (check_opening_stock_balance = 'Zero Stock Balance') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'No Weekly Demand') then 'Consider Disabling'
                          when (check_opening_stock_balance = 'Zero Stock Balance') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'No Weekly Demand') then 'Consider Disabling'
                          
                          ---------------- Out Of Stock --------------
                          when (check_opening_stock_balance = 'Zero Stock Balance') and (check_latest_pr = 'Has Latest PR') and (check_weekly_demand = 'Has Weekly Demand') then 'Out Of Stock'
                          when (check_opening_stock_balance = 'Zero Stock Balance') and (check_latest_pr = 'No Latest PR') and (check_weekly_demand = 'Has Weekly Demand') then 'Out Of Stock'

                        else 'UNSET' end as stock_position_status,
                        from get_re_order_details_cte usr
                        ),
get_recommendations_cte as (
  select *,
  case
    when (stock_position_status = 'Out Of Stock') and (adjusted_opening_stock_cover_days = 0) then (opening_stock_balance_valuation_rate * daily_demand_qty)
    /*when (stock_position_status = 'Out Of Stock') and (check_opening_stock_balance = 'With Stock Balance Qty' and (opening_stock_cover_days = 0)) then (opening_stock_balance_valuation_rate* daily_demand_qty*3)
    when (stock_position_status = 'Out Of Stock') and (check_opening_stock_balance = 'With Stock Balance Qty' and (opening_stock_cover_days = 1)) then (opening_stock_balance_valuation_rate* daily_demand_qty*2)
    when (stock_position_status = 'Out Of Stock') and (check_opening_stock_balance = 'With Stock Balance Qty' and (opening_stock_cover_days = 2)) then (opening_stock_balance_valuation_rate* daily_demand_qty)*/
    when (stock_position_status = 'Out Of Stock') then (adjusted_stock_deficit * opening_stock_balance_valuation_rate)
  else 0 end as out_of_stock_value,
  case
    when (stock_position_status = 'SLOB') then (opening_stock_balance_value - maximum_7_day_stock_value) 
  else 0 end as slob_value_7_days,
  case
    when (stock_position_status = 'To Be Disabled') then 'To Be Disabled; Clear Existing Stock'
    when (stock_position_status = 'To Be Activated') then 'To Be Activated; By The Territory Manager'

    when (stock_position_status = 'Dead Stock') then 'Dead Stock; To Be Disabled'
    when (stock_position_status = 'Consider Disabling') then 'Consider Disabling; No Weekly Demand'
    when (stock_position_status = 'SLOB') then 'SLOB; Hold On Pending Orders'
    when (stock_position_status = '4-7 Days Stock') then 'Low Stock; Expediate LPO Approval & Monitor Supplier'
    when (stock_position_status = 'Out Of Stock') then 'Out Of Stock; Expediate LPO Approval & Monior Supply'
  else 'UNSET' end as recommendation,
  from get_stock_position_status_cte
  ),
stock_replenishment_report as (
                                select *,
                                case
                                  when (stock_position_status = 'SLOB') then slob_value_7_days
                                  when (stock_position_status = 'Out Of Stock') then out_of_stock_value
                                  when (stock_position_status = '4-7 Days Stock') then opening_stock_balance_value
                                  when (stock_position_status = 'To Be Activated') then opening_stock_balance_value
                                  when (stock_position_status = 'To Be Disabled') then opening_stock_balance_value
                                  when (stock_position_status = 'Dead Stock') then opening_stock_balance_value
                                  when (stock_position_status = 'Consider Disabling') then opening_stock_balance_value
                                  when (stock_position_status = 'New Listing') then opening_stock_balance_value
                                  --when (stock_position_status = 'Consider Disabling') then opening_stock_balance_value
                                else 0 end as stock_value
                                from get_recommendations_cte
                                )
/*,
---------------------------- QA --------------------
stock_replenishments_agg_cte as (
                                  select distinct --company_id,
                                  warehouse,
                                  --original_territory_id,
                                  new_territory_id,
                                  stock_position_status,
                                  sum(opening_stock_balance_value) as opening_stock_balance_value
                                  from stock_replenishment_model_with_recommendations_cte
                                  WHERE original_territory_id = 'Kiambu'
                                  group by 1,2,3
                                  order by opening_stock_balance_value desc
                                  )*/
------------ Report --------------------------
--select * from opening_stock_balance_cte
--select * from stock_replenishments_agg_cte
select * from stock_replenishment_report --WHERE original_territory_id = 'Kiambu' --and item_code = 'Hit A4 Exercise Books 48 Pages Square Line'
where stock_position_status = 'Out Of Stock' --and out_of_stock_value = 0
--where stock_position_status = 'SLOB'
--select distinct check_opening_stock_balance, check_latest_pr, check_weekly_demand from opening_stock_with_purchase_history_cte WHERE original_territory_id = 'Kiambu' order by 1,2,3--and item_code = 'Prestige Original Margarine 250g'
--where item_code = 'Movit Hair Avocado Oil 100gm' and new_territory_id = 'Ruiru'
--where FORMAT_DATE('%Y%m%d', opening_stock_balance_date) between @DS_START_DATE and @DS_END_DATE
order by opening_stock_balance_date, warehouse, item_code
