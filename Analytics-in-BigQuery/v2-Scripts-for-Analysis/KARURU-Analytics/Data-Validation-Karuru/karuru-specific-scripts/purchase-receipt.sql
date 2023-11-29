------------------- Karuru --------------
------------- Purchase Receipt --------------------
with
karuru_pr as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              ),
pr_items as (
              select distinct posting_date,
              company_id,
              territory_id,
              pri.item_id,
              pri.item_code,
              pri.item_name,
              pri.uom,
              --pri.item_group_id,
              --pri.brand
              avg(rate) as rate,
              sum(received_qty) as received_qty,
              sum(amount) as amount
              from karuru_pr pr, unnest(items) as pri
              where index = 1
              and buying_type in ('PURCHASING')
              and workflow_state in ('COMPLETED')
              and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
              and territory_id in ('Kawempe', 'Luzira', 'Mukono')
              and item_code = 'Velvex Air Freshener Lavender And Chamomile 300ML'
              --and item_id = 'Everyday Milk Chocolate Biscuits 8.5g'
              group by 1,2,3,4,5,6,7
              ),
pr_report as (
              select distinct territory_id,
              item_id,
              item_code,
              item_name,
              LAST_VALUE(posting_date) OVER (PARTITION BY company_id,territory_id, item_code ORDER BY posting_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_posting_date,
              LAST_VALUE(rate) OVER (PARTITION BY company_id,territory_id, item_code ORDER BY posting_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_rate
              from pr_items
              order by territory_id,item_code
              )
select *
from pr_items