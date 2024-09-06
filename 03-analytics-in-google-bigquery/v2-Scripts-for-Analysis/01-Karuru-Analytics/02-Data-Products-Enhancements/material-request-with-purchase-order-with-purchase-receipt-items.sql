----------- Material Requests, Purchase Orders, & Purchase Receipt Items ---------------
with
----------------------------- Mayterial Requests ---------------------------------------------------------------
material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    --where date(date_created) >= date_sub(date_trunc(current_date, month), interval 6 month)
                    where date(date_created) >= '2024-06-01'
                  ),
material_request_items as (
                            select distinct date(mr.date_created) as date_created,
                            --mr.date_created as m,
                            mr.company_id,
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
                            --sum(mri.qty) as qty,
                            sum(mri.ordered_qty) as ordered_qty ,
                            sum(mri.received_qty) as received_qty,
                            --mri.rate,
                            --mri.amount                                
                            from material_request mr, unnest(items) mri
                            where index = 1
                            and material_request_type = 'PURCHASE'
                            group by 1,2,3,4,5,6,7,8,9,10,11
                            ),
------------------- Purchase Order --------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    --where date(creation) >= date_sub(date_trunc(current_date, month), interval 24 month)
                    where date(creation) >= '2024-06-01'
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
              --where date(date_created) >= date_sub(date_trunc(current_date, month), interval 24 month)
              where date(date_created) >= '2024-06-01'
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
                            --pri.item_group_id,
                            --pri.brand
                            --supplier_name,
                            --avg(rate) as rate,
                            sum(received_qty) as received_qty,
                            --sum(amount) as amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            group by 1,2,3,4,5,6,7,8,9,10,11
                            ),
--------------------------- Purchasing cte ---------------------------
purchasing_cte as (
                    select distinct date(mri.date_created) as creation_date_of_material_request,
                    date(poi.creation) as creation_date_of_purchase_order,
                    date(pri.date_created) as creation_date_of_purchase_receipt,
                    date_diff(date(pri.date_created), date(poi.creation), day) as calculated_lead_time,
                    mri.company_id,
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
                    mri.received_qty as material_request_received_qty,
                    pri.received_qty as purchase_receipt_received_qty,
                    pri.supplier
                    from material_request_items mri
                    left join purchase_receipt_items pri on mri.warehouse_id = pri.set_warehouse_id and  mri.id = pri.material_request_id and mri.item_code = pri.item_id and mri.uom = pri.stock_uom
                    left join purchase_order_items poi on poi.id = pri.purchase_order and poi.warehouse_id = pri.set_warehouse_id and poi.item_code_id = pri.item_id
                    ),
pending_and_completed_purchasing_report as (
                                            select *
                                            FROM purchasing_mashup
                                            WHERE workflow_state_of_purchase_receipt not in ('REJECTED') 
                                            ),
get_latest_suppliers as (
          select distinct company_id,
          territory_id,
          warehouse_id,
          item_code,
          last_value(supplier)over(partition by territory_id, item_code order by creation_date_of_material_request asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_supplier,
          last_value(creation_date_of_purchase_receipt)over(partition by territory_id, item_code order by creation_date_of_material_request asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_posting_date
          from pending_and_completed_purchasing_report
          ),
get_avg_lead_time as (
                  select distinct company_id,
                  warehouse_id,
                  territory_id,
                  supplier,
                  round(avg(calculated_lead_time)over(partition by territory_id, supplier order by creation_date_of_purchase_receipt asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as calculated_lead_time
                  from pending_and_completed_purchasing_report
                  ),
get_latest_purchas_receipt_received_qty as (
                            select distinct creation_date_of_purchase_receipt,
                            company_id,
                            warehouse_id,
                            territory_id,
                            item_code,
                            uom,
                            sum(purchase_receipt_received_qty) as purchase_receipt_received_qty,
                            from pending_and_completed_purchasing_report
                            where workflow_state_of_purchase_receipt in ('COMPLETED')
                            group by 1,2,3,4,5,6
                            ),
get_pending_material_request as (
                            select distinct creation_date_of_material_request,
                            company_id,
                            warehouse_id,
                            territory_id,
                            item_code,
                            uom,
                            count(distinct material_request_id) as count_pending_material_requests,
                            sum(ordered_qty_of_material_request) as pending_qty_of_material_request,
                            --sum(purchase_receipt_received_qty) as purchase_receipt_received_qty,
                            from pending_and_completed_purchasing_report
                            where workflow_state_of_purchase_receipt in ('PENDING')
                            group by 1,2,3,4,5,6
                            )
                           
select *
--distinct workflow_state_of_material_request, workflow_state_of_purchase_receipt

--from pending_and_completed_purchasing_report
--from get_avg_lead_time
from get_pending_material_request
where warehouse_id not in ('Test KE Main - KDKE')
--and FORMAT_DATE('%Y%m%d', date(creation_datetime_of_material_request)) between @DS_START_DATE and @DS_END_DATE  
and company_id =  'KYOSK DIGITAL SERVICES LTD (KE)'
--and compnay_id = 'YOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
--and territory_id in ('Eastlands')
--and id in ("MAT-MR-2024-12141", "MAT-MR-2024-08877") 
order by 1,2