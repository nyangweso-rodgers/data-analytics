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
material_request_items_cte as (
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
------------------- Purchase Order --------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    where date(creation) >= date_sub(current_date, interval 12 month)
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
----------------------- Purchase Receipt Item ---------------------------
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              where date(date_created) >= date_sub(current_date, interval 12 month)
              and territory_id not in ('Test UG Territory', 'Test KE Territory', 'Kawangware', 'Juja', 'Ongata Rongai', 'Kisii', 'Nakuru', 'Athi River', 'Karatina', 'Eldoret', 'Thika Rd', 'Mtwapa Mombasa', 'Ruai', 'Kiambu')
              --and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
              ),
purchase_receipt_items_cte as (
                            select distinct date_created,
                            cast(posting_date as date) as posting_date,
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
            else 'UNSET' end as statuses_description,

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
                              select distinct current_datetime as scheduled_query_creation_time,
                              'rodgers.nyangweso@kyosk.app' as scheduled_query_created_by,
                              mr_creation_date,
                              po_creation_date,

                              company_id,
                              warehouse_id,
                              territory_id,
                              statuses_description,
                              material_request_id,
                              purchase_order_id,
                              item_code,
                              stock_uom,
                              mr_stock_qty,
                              mr_qty,
                              from mr_po_and_pr_cte
                              where statuses_description in ('MR In Draft; PO In Pending; PR Is Null', 'MR In Draft; PO Is Null; PR Is Null', 'MR In Received; PO In Approved; PR Is Null')
                              ),
pending_mr_po_and_pr_agg_cte as (
                                  select distinct company_id,
                                  warehouse_id,
                                  territory_id,
                                  item_code,
                                  stock_uom,
                                  sum(case when statuses_description = 'MR In Draft; PO In Pending; PR Is Null' then mr_stock_qty else 0 end) as mr_stock_qty_in_draft_with_pending_po,
                                  sum(case when statuses_description = 'MR In Draft; PO Is Null; PR Is Null' then mr_stock_qty else 0 end) as mr_stock_qty_in_draft_with_null_po,
                                  sum(case when statuses_description = 'MR In Ordered; PO In Approved; PR Is Null' then mr_stock_qty else 0 end) as  mr_stock_qty_in_ordered_with_approved_po,
                                  sum(case when statuses_description = 'MR In Received; PO In Approved; PR Is Null' then mr_stock_qty else 0 end) as  mr_stock_qty_in_received_with_approved_po,
                                  max(mr_creation_date) as pending_mr_max_creation_date
                                  from mr_po_and_pr_cte
                                  group by 1,2,3,4,5
                                  ),
------------------- Latest MR, PO and PR ---------------------------------
latest_mr_po_and_pr_cte as (
                select distinct company_id,
                warehouse_id,
                territory_id,
                item_code,
                stock_uom,
                last_value(mr_creation_date IGNORE NULLS)over(partition by warehouse_id, item_code order by mr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_creation_date,
                last_value(mr_item_group IGNORE NULLS)over(partition by warehouse_id, item_code order by mr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_mr_item_group,

                last_value(date(po_creation_date) IGNORE NULLS)over(partition by warehouse_id, item_code order by po_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_creation_date,
                last_value(po_supplier IGNORE NULLS)over(partition by warehouse_id, item_code order by po_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_po_supplier,

                last_value(pr_posting_date IGNORE NULLS)over(partition by warehouse_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_creation_date,
                last_value(pr_posting_date IGNORE NULLS)over(partition by warehouse_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_posting_date,
                last_value(pr_supplier IGNORE NULLS)over(partition by territory_id, item_code order by pr_creation_datetime asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_pr_supplier,
                from mr_po_and_pr_cte
                      )                   
select * from pending_mr_po_and_pr_cte