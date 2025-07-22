------------------------- PR & PO ------------------------
with
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              --WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              where date(date_created) >= date_sub(date_trunc(current_date, month), interval 6 month)
              --where date(date_created) >= '2022-01-01'
              --WHERE date(date_created) < current_date
              ),
purchase_receipt_items as (
                            select distinct date(date_created) date_created,
                            posting_date,
                            --date_diff(date(posting_date), date(date_created), day) as date_delta,
                            company_id,
                            territory_id,
                            name,
                            pr.workflow_state,
                            i.purchase_order,
                            i.item_id,
                            i.item_code,
                            --i.item_name,
                            --i.uom,
                            --i.conversion_factor,
                            i.stock_uom,
                            --pri.item_group_id,
                            --pri.brand
                            --supplier,
                            --supplier_name,
                            --avg(rate) as rate,
                            --sum(received_qty) as received_qty,
                            --sum(amount) as amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            and buying_type in ('PURCHASING')
                            and workflow_state not in ('REJECTED')
                            --group by 1,2,3,4,5,6,7,8,9,10,11
                            ),
----------------------------------- PO -----------------------------------------
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    --WHERE date(creation) < current_date
                    where date(creation) >= date_sub(date_trunc(current_date, month), interval 24 month)
                    ),
purchase_order_items as (
                          select distinct date(creation) as creation,
                          date(purchase_order_date) as purchase_order_date,
                          --date_diff(date(purchase_order_date), date(creation), day) as date_delta,
                          fulfillment_date,
                          id,
                          --purchase_order_no,
                          --i.warehouse_id,
                          --i.item_group,
                          i.item_code_id,
                          --i.item_name,
                          --sum(i.qty) as qty,
                          --sum(i.stock_qty) as stock_qty,
                          --i.item_group
                          from purchase_order po, unnest(items) i
                          where index =1
                          --group by 1,2,3,4,5,6,7,8,9,10
                          ),
----------------- Mashup ----------------------
purchase_recept_and_purchase_order as (
                                        select 
                                        distinct poi.purchase_order_date,
                                        poi.creation as purchase_order_creation_date,
                                        pri.date_created as purchase_receipt_creation_date,
                                        pri.posting_date as purchase_receipt_posting_date,
                                        date_diff(date(pri.posting_date), poi.creation, day) as lead_time,
                                        pri.company_id,
                                        pri.territory_id,
                                        pri.name as purchase_receipt_name,
                                        pri.workflow_state as purchase_receit_workflow_state,
                                        pri.purchase_order,
                                        pri.item_code,
                                        pri.stock_uom
                                        from purchase_receipt_items pri
                                        left join purchase_order_items poi on pri.purchase_order = poi.id and pri.item_code = poi.item_code_id
                                        )
select *
from purchase_recept_and_purchase_order
where territory_id not in ('Karatina','Nakuru', 'Kyosk HQ', 'Test KE Territory')
and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
--and purchase_order_date is null
and territory_id = 'Nyeri'
and item_code = 'Movit Blowout 150gm'