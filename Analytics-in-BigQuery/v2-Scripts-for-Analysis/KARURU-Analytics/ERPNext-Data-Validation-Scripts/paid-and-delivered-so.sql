-------------- ERPNext - SO ---------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            where date(creation) between '2023-07-01' and '2023-07-31'
                            and workflow_state in ('PAID')
                            --and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
                            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                            and company= 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                            ),
sales_order_summary as (
                        select 
                        --distinct company, count(distinct name) as so_summary, sum(grand_total) as grand_total
                        distinct so.company, so.name, so.grand_total,
                        from sales_order_with_index so
                        where index = 1
                        --group by 1
                        ),
sales_order_items as (
                      select distinct so.company, so.name,sum(amount)
                      --distinct  company, count(distinct so.name) as so_items, sum(amount) as items_amount
                      from sales_order_with_index so, unnest(items) soi
                      where index = 1
                      and fulfilment_status =  "PAID"
                      group by 1,2
                      )
select sos.*,
soi.*except(company, name)
from sales_order_summary sos
left join sales_order_items soi on sos.company = soi.company and sos.name = soi.name