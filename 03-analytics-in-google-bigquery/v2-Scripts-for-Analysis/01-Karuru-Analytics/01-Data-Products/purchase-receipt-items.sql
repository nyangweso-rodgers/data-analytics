
------------- Purchase Receipt Item --------------------
with
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              --WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              where date(date_created) >= '2021-01-01'
              ),
purchase_receipt_items as (
                            select distinct posting_date,
                            date_created,
                            company_id,
                            territory_id,
                            name,
                            buying_type,
                            workflow_state,
                            i.purchase_order,

                            i.item_id,
                            i.item_code,
                            i.item_name,
                            i.uom,
                            i.conversion_factor,
                            i.stock_uom,
                            i.item_group_id,
                            --pri.brand
                            supplier,
                            supplier_name,
                            --avg(rate) as rate,
                            --sum(received_qty) as received_qty,
                            --sum(amount) as amount
                            supplier_group
                            from purchase_receipt pr, unnest(items) as i
                            where index = 1
                            
                            --and workflow_state in ('COMPLETED')
                            
                            --and territory_id in ('Kawempe', 'Luzira', 'Mukono')
                            --and item_code = 'Velvex Air Freshener Lavender And Chamomile 300ML'
                            --and item_id = 'Everyday Milk Chocolate Biscuits 8.5g'
                            --group by 1,2,3,4,5,6,7
                            )
select 
--distinct item_code, item_group_id--count(distinct item_group_id) as item_group_id
distinct company_id, territory_id, supplier, max(posting_date) as last_posting_date
--distinct workflow_state, max(posting_date), count(distinct name)
--distinct name, posting_date, date(date_created) as date_created, date_diff(date(posting_date), date(date_created), day) as date_diff
--distinct name, workflow_state
from purchase_receipt_items
where buying_type in ('PURCHASING')
--and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
and company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')
and territory_id = 'Nyeri'
and item_code = 'Movit Blowout 150gm'
group by 1,2,3
order by company_id, territory_id, supplier
--having item_group_id > 1