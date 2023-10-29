-------------- ERPNext --------
---------- SO ---------------------
with
erp_so as (
            SELECT *,
            row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            where date(creation) between '2023-10-01' and  '2023-10-25'
            --where date(creation) = '2023-10-25'
            --and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
            ),
so_report as (
              select distinct status,
              name
              from erp_so so
              where index = 1
              --and status = 'Draft'
              --and name in ('SAL-ORD-EMBRGNL', 'SAL-ORD-KISWU5H')
              )
select distinct status,
count(distinct name) as names
from so_report
group by 1
order by 2 desc