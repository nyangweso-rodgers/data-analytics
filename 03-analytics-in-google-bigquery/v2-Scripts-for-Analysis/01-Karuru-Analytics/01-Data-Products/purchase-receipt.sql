
------------- Purchase Receipt --------------------
with
purchase_receipt as (
              SELECT *,
              row_number()over(partition by id order by date_modified desc) as index
              FROM `kyosk-prod.karuru_reports.purchase_receipt` 
              WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
              --where date(date_created) >= '2022-01-01'
              ),
purchase_receipt_report as (
                            select distinct posting_date,
                            date_modified,
                            bq_upload_time,
                            company_id,
                            territory_id,
                            name,
                            supplier,
                            supplier_name,
                            --avg(rate) as rate,
                            --sum(received_qty) as received_qty,
                            --sum(amount) as amount
                            supplier_group
                            from purchase_receipt pr
                            where index = 1
                            and buying_type in ('PURCHASING')
                            --and workflow_state in ('COMPLETED')
                            and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)')
                            --and territory_id in ('Kawempe', 'Luzira', 'Mukono')
                            --and item_code = 'Velvex Air Freshener Lavender And Chamomile 300ML'
                            --and item_id = 'Everyday Milk Chocolate Biscuits 8.5g'
                            --group by 1,2,3,4,5,6,7
                            )
select max(date_modified), max(bq_upload_time)
from purchase_receipt_report