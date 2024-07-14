with
purchase_order as (
                    SELECT *,
                    row_number()over(partition by id  order by modified desc) as index
                    FROM `kyosk-prod.karuru_reports.purchase_order` 
                    --WHERE TIMESTAMP_TRUNC(creation, DAY) > TIMESTAMP("2022-02-05")
                    WHERE date(creation) >= date_trunc(current_date, month)
                    ),
purchase_order_items as (
                          select distinct date(creation) as creation,
                          date(purchase_order_date) as purchase_order_date,
                          fulfillment_date,
                          id,
                          purchase_order_no,
                          i.warehouse_id,
                          i.item_group,
                          i.item_code_id,
                          i.item_name,
                          sum(i.qty) as qty,
                          sum(i.stock_qty) as stock_qty,
                          --i.item_group
                          from purchase_order po, unnest(items) i
                          where index =1
                          group by 1,2,3,4,5,6,7,8,9
                          ),
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              --WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              where date(date_created) >= '2022-01-01'
              ),
purchase_receipt_items as (
                            select distinct date(date_created) date_created,
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
                            --i.stock_uom,
                            --pri.item_group_id,
                            --pri.brand
                            supplier,
                            --supplier_name,
                            --avg(rate) as rate,
                            sum(received_qty) as received_qty,
                            sum(amount) as amount
                            --supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            and buying_type in ('PURCHASING')
                            group by 1,2,3,4,5,6,7,8,9,10,11
                            ),
purchase_order_and_purchase_receipt as (
                                        select distinct poi.creation as purchase_order_creation_date,
                                        pri.date_created as purchase_receipt_creation_date,
                                        poi.purchase_order_date,
                                        poi.fulfillment_date as purcase_order_fulfillment_date,
                                        pri.posting_date as purchase_receipt_posting_date,
                                        poi.id,
                                        poi.purchase_order_no,
                                        poi.warehouse_id,
                                        poi.item_code_id,
                                        poi.item_name,
                                        poi.qty as purchase_order_qty,
                                        poi.stock_qty as purchase_order_stock_qty,
                                        pri.received_qty as purchase_receipt_received_qty,
                                        pri.amount as purchase_receipt_amount,
                                        pri.company_id,
                                        pri.name as purchase_receipt_name,
                                        pri.supplier,
                                        pri.workflow_state as purchase_receipt_workflow_state
                                        from purchase_order_items poi
                                        left join purchase_receipt_items pri on poi.purchase_order_no = pri.purchase_order and poi.item_code_id = pri.item_code
                                        )
select *
from purchase_order_and_purchase_receipt
where company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
--where company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
order by purchase_order_date desc, warehouse_id