----------- Material Requests, Purchase Orders, & Purchase Receipt Items ---------------
with

material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    where date(date_created) >= date_sub(current_date, interval 12 month)
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
                            select distinct --date(mr.date_created) as date_created,
                            mr.date_created,
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
                            and warehouse_id not in ('Test KE Main - KDKE')
                            and target_warehouse_territory_id not in ('Karatina', 'Eldoret', 'Ongata Rongai', 'Athi River', 'Kawangware', 'Juja', 'Thika Rd', "Ruai", 'Kisii', 'Meru')
                            ),
get_latest_mr_report as (
                          select distinct warehouse_id,
                          --item_id,
                          item_code,
                          --item_name,
                          last_value(date(date_created))over(partition by warehouse_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_creation_date,
                          last_value(item_group)over(partition by warehouse_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_item_group,
                          from material_request_items
                          ),
------------------- Purchase Order --------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where date(creation) >= date_sub(current_date, interval 12 month)
                    ),
purchase_order_items as (
                          select distinct creation,
                          --po.company,
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
get_latest_po_report as (
                          select distinct warehouse_id,
                          item_code_id,
                          last_value(date(creation))over(partition by warehouse_id, item_code_id order by creation asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_creation_date,
                          last_value(supplier)over(partition by warehouse_id, item_code_id order by creation asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_supplier,
                          from purchase_order_items
                          ),
----------------------- Purchase Receipt Item ---------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              where date(date_created) >= date_sub(current_date, interval 12 month)
              ),
purchase_receipt_items as (
                            select distinct date_created,
                            posting_date,
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
                            --pri.item_group_id,
                            --pri.brand

                            pr.supplier,
                            --supplier_name,

                            i.received_qty,
                            i.amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            ),
get_latest_pr_report as (
                          select distinct set_warehouse_id,
                          territory_id,
                          item_code,
                          last_value(posting_date)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_posting_date,
                          last_value(supplier)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_supplier,
                          from purchase_receipt_items
                          ),
------------------------------------------- Mashup ----------------------------
mr_with_po_with_pr_mashup as (
            select distinct date(mri.date_created) as mr_creation_date,
            mri.company_id as company_id,
            mri.warehouse_id as warehouse_id,
            --mri.warehouse_id as mr_warehouse_id,
            --mri.set_warehouse_id as mr_set_warehouse_id,
            --mri.target_warehouse_territory_id as mr_target_warehouse_territory_id,
            mri.target_warehouse_territory_id as territory_id,

            mri.id as material_request_id,
            mri.name as material_request,
            mri.workflow_state as mr_workflow_state,
            mri.status as mr_status,

            coalesce(mri.item_group, glmrr.latest_mr_item_group) as item_group,
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

            poi.warehouse_id as po_warehouse_id,
            poi.id as purchase_order_id,
            poi.purchase_order_no,
            poi.workflow_state as po_workflow_state,
            --poi.item_group as po_item_group,
            --poi.item_code_id as po_item_code_id,
            --poi.stock_uom as po_stock_uom,
            poi.supplier as po_supplier,

            pri.id as purchase_receipt_id,
            pri.workflow_state as pr_workflow_state,
            pri.received_qty as pr_received_qty,

            glmrr.latest_mr_creation_date,
            --glmrr.latest_mr_item_group,

            glpor.latest_po_creation_date,
            glpor.latest_po_supplier,

            plprr.latest_pr_posting_date,
            plprr.latest_pr_supplier,
            coalesce(plprr.latest_pr_supplier, glpor.latest_po_supplier) as supplier
            from material_request_items mri
            left join get_latest_mr_report glmrr on mri.warehouse_id = glmrr.warehouse_id and mri.item_code = glmrr.item_code
            left join purchase_order_items poi on mri.id = poi.material_request and mri.item_code = poi.item_code_id and mri.stock_uom = poi.stock_uom
            left join get_latest_po_report glpor on mri.warehouse_id = glpor.warehouse_id and mri.item_code = glpor.item_code_id
            left join purchase_receipt_items pri on poi.id = pri.purchase_order and poi.item_code_id = pri.item_code and poi.stock_uom = pri.stock_uom
            left join get_latest_pr_report plprr on mri.warehouse_id = plprr.set_warehouse_id and  mri.item_code = plprr.item_code
            )                          
select *
from mr_with_po_with_pr_mashup
--from mr_with_po_with_pr_mashup
--where 
--and mr_company_id =  'KYOSK DIGITAL SERVICES LTD (KE)'
--and material_request_creation_date between '2024-08-01' and '2024-08-31'
--and FORMAT_DATE('%Y%m%d', material_request_creation_date) between @DS_START_DATE and @DS_END_DATE  
--and compnay_id = 'YOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
--and purchase_order_no in ('PUR-ORD-2024-06989')
--
--where item_code = 'Halisi Fry Cooking Oil 20L'
order by mr_creation_date desc, company_id, warehouse_id, item_code 