------------------- Karuru --------------
------------- Purchase Receipt --------------------
with
karuru_pr as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              --WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              where date(date_created) >= '2022-01-01'
              ),
pr_items as (
              select distinct posting_date,
              company_id,
              territory_id,
              name,
              i.item_id,
              i.item_code,
              i.item_name,
              i.uom,
              i.conversion_factor,
              i.stock_uom,
              --pri.item_group_id,
              --pri.brand
              supplier,
              supplier_name,
              --avg(rate) as rate,
              --sum(received_qty) as received_qty,
              --sum(amount) as amount
              supplier_group
              from karuru_pr pr, unnest(items) as i
              where index = 1
              and buying_type in ('PURCHASING')
              --and workflow_state in ('COMPLETED')
              --and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
              --and territory_id in ('Kawempe', 'Luzira', 'Mukono')
              --and item_code = 'Velvex Air Freshener Lavender And Chamomile 300ML'
              --and item_id = 'Everyday Milk Chocolate Biscuits 8.5g'
              --group by 1,2,3,4,5,6,7
              )
select distinct posting_date, supplier_group, name
--distinct supplier_group, count(distinct name)
from pr_items
--where supplier_group not in ('Manufacturer', 'Distributor', 'Local')
--group by 1
--order by 2 desc
where (supplier_group is null) or (supplier_group = '')