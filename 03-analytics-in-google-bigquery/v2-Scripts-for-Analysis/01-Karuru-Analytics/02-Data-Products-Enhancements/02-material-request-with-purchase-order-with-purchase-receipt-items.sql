----------- Material Requests, Purchase Orders, & Purchase Receipt Items ---------------
with
--------------------------------- Upload - Skus To Be Deleted -------------------------
uploaded_skus_to_be_disabled as (
                                SELECT distinct company_id, 
                                item_code, 
                                status 
                                FROM `kyosk-prod.karuru_upload_tables.skus_to_be_disabled` 
                                ),
---------------------------------- Material Requests --------------------------
material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    where date(date_created) >= date_sub(current_date, interval 12 month)
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
                              sum(case when status = 'DRAFT' then qty else 0 end) as qty_in_draft_status,
                              sum(case when status = 'PARTIALLY_ORDERED' then ordered_qty else 0 end) as ordered_qty_in_partially_ordered_status,
                              sum(case when status = 'ORDERED' then ordered_qty else 0 end) as ordered_qty_in_ordered_status,
                              string_agg(distinct status, "/" order by status) as status,
                              --sum(ordered_qty) as ordered_qty,
                              max(date(date_created)) as max_creation_date
                              from material_request_items
                              where status in ('DRAFT', 'ORDERED', 'PARTIALLY_ORDERED')
                              group by 1,2,3
                              ),
------------------- Purchase Order --------------------------
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
                          select distinct company,
                          warehouse_id,
                          --warehouse_territory,
                          territory,
                          item_code_id,
                          last_value(date(creation))over(partition by territory, item_code_id order by creation asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_creation_date,
                          last_value(supplier)over(partition by territory, item_code_id order by creation asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_supplier,
                          from purchase_order_items
                          ),
----------------------- Purchase Receipt Item ---------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              where date(date_created) >= date_sub(current_date, interval 12 month)
              and territory_id not in ('Test UG Territory', 'Test KE Territory', 'Kawangware', 'Juja', 'Ongata Rongai', 'Kisii', 'Nakuru', 'Athi River', 'Karatina', 'Eldoret', 'Thika Rd', 'Mtwapa Mombasa', 'Ruai', 'Kiambu')
              and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
              ),
purchase_receipt_items as (
                            select distinct date_created,
                            posting_date,
                            --posting_time,
                            company_id,
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
                          select distinct company_id,
                          set_warehouse_id,
                          territory_id,
                          item_code,
                          last_value(date(date_created))over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_creation_date,
                          last_value(posting_date)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_posting_date,
                          last_value(supplier)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_supplier,
                          last_value(item_group_id)over(partition by territory_id, item_code order by date_created asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_item_group_id,
                          from purchase_receipt_items
                          ),
------------------------------------------- Mashup ----------------------------
mr_with_po_with_pr_mashup as (
            select distinct date(mri.date_created) as mr_creation_date,
            lmr.latest_mr_creation_date,
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

            coalesce(mri.item_group, lmr.latest_mr_item_group) as item_group,
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

            poi.territory as po_territory,
            date(poi.creation) as po_creation_date,
            poi.warehouse_id as po_warehouse_id,
            poi.id as purchase_order_id,
            poi.purchase_order_no,
            poi.workflow_state as po_workflow_state,
            --poi.item_group as po_item_group,
            --poi.item_code_id as po_item_code_id,
            --poi.stock_uom as po_stock_uom,
            poi.supplier as po_supplier,

            date(pri.date_created) as pr_creation_date,
            pri.id as purchase_receipt_id,
            pri.workflow_state as pr_workflow_state,
            pri.received_qty as pr_received_qty,

            --glmrr.latest_mr_item_group,

            lpo.latest_po_creation_date,
            lpo.latest_po_supplier,

            lpr.latest_pr_posting_date,
            lpr.latest_pr_supplier,
            coalesce(lpr.latest_pr_supplier, lpo.latest_po_supplier) as supplier
            from material_request_items mri
            left join latest_material_requests_cte lmr on mri.warehouse_id = lmr.warehouse_id and mri.item_code = lmr.item_code
            left join purchase_order_items poi on mri.id = poi.material_request and mri.item_code = poi.item_code_id and mri.stock_uom = poi.stock_uom
            left join latest_purchase_order_cte lpo on mri.warehouse_id = lpo.warehouse_id and mri.item_code = lpo.item_code_id
            left join purchase_receipt_items pri on poi.id = pri.purchase_order and poi.item_code_id = pri.item_code and poi.stock_uom = pri.stock_uom
            left join latest_purchase_receipt_cte lpr on mri.warehouse_id = lpr.set_warehouse_id and  mri.item_code = lpr.item_code
            )                          
--select * from material_request_items where date(date_created) between '2024-08-28' and '2024-09-10'
--select * from latest_material_requests_cte
--select distinct company, warehouse_id, warehouse_territory, territory from purchase_order_items order by 1,2

--select * from latest_purchase_receipt_cte

--select * from latest_purchase_order_cte
--select * from mr_with_po_with_pr_mashup where mr_creation_date between '2024-09-01' and '2024-09-10'
--select * from material_request_items where item_code = 'Ideal Scented Petroleum Jelly 50gms'
select *  from pending_material_requests_cte
--where FORMAT_DATE('%Y%m%d', mr_creation_date) between @DS_START_DATE and @DS_END_DATE  
--where mr_creation_date between '2024-08-28' and '2024-09-10'
--and compnay_id = 'YOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
--and purchase_order_no in ('PUR-ORD-2024-06989')
--
--where item_code = 'Halisi Fry Cooking Oil 20L'
--order by mr_creation_date desc, company_id, warehouse_id, item_code 