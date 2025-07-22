
------------- Purchase Receipt Item --------------------
with
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              WHERE buying_type in ('PURCHASING')
              and date(date_created) >= date_sub(date_trunc(current_date, month), interval 3 month)
              --and date(date_created) >= '2021-01-01'
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
                            )
select distinct territory_id, item_code, count(distinct  supplier) as supplier
--distinct item_code, item_group_id--count(distinct item_group_id) as item_group_id
--distinct workflow_state, max(posting_date), count(distinct name)
--distinct name, posting_date, date(date_created) as date_created, date_diff(date(posting_date), date(date_created), day) as date_diff
--distinct name, workflow_state
from purchase_receipt_items
--and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
where company_id in ('KYOSK DIGITAL SERVICES LTD (KE)')

--and territory_id = 'Majengo Mombasa'
and item_code like "/*Sugar*/"
--and item_code  in ('Zuri Packed Sugar 1kg', 'Taifa Maize Flour 1kg')
--and item_code = 'Taifa Maize Flour 1kg'
--order by company_id, territory_id, supplier
group by 1,2
having supplier > 1