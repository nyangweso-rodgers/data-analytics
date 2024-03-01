------------------- Karuru --------------
------------- Purchase Receipt Items --------------------
with
karuru_pr as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              --WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              where date(date_created) >= '2022-01-01'
              ),
pr_items as (
              select distinct company_id,
              name,
              i.item_id,
              i.item_code,
              i.item_name,
              i.conversion_factor,
              i.stock_uom,
              from karuru_pr pr, unnest(items) as i
              where index = 1
              and buying_type in ('PURCHASING')
              and workflow_state in ('COMPLETED')
              --and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
              and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
              )
select distinct item_id, item_code,
from pr_items
order by 1