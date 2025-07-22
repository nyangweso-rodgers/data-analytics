  ----------------------- ERPNext ---------------
  ------------------- Paid & Delivered DNs --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where  territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            and workflow_state in ('PAID', 'DELIVERED')
            --and posting_date between '2022-08-01' and '2022-08-31'
            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
            and date(creation) = '2022-08-17'
            ),
monthly_lists as (
                  select distinct company, 
                  posting_date,
                  dn.name,
                  dn.workflow_state,
                  dni.item_code,
                  dni.uom,
                  dni.base_amount
                  from erp_dns dn, unnest(items) dni
                  where index = 1
                  and dn.name = "DN-MWEN-6XJR"
                  ),
monthly_agg as (
                  select distinct company, 
                  date_trunc(posting_date, month) as posting_month,
                  count(distinct dn.name) as dns,
                  count(distinct customer) as customers,
                  sum(grand_total) as grand_total
                  from erp_dns dn 
                  where index = 1
                  group by 1,2
                  order by 2
                  ),
items_monthly_agg as(
                      select distinct company, 
                      date_trunc(posting_date, month) as posting_month,
                      count(distinct dn.name) as dns,
                      count(distinct customer) as customers,
                      sum(base_amount) as base_amount
                      from erp_dns dn, unnest(items) dni  
                      where index = 1
                      group by 1,2
                      order by 2
                      )
select *
from monthly_lists