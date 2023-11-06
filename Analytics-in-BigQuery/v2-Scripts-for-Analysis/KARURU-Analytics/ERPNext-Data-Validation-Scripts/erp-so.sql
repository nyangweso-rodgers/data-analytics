-------------- ERPNext --------
---------- SO ---------------------
with
erp_so as (
            SELECT *,
            --row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            --where date(creation) between '2022-02-01' and  '2023-10-25'
            --where date(creation) = '2022-11-11'
            --and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            --and workflow_state not in ('INITIATED', 'USER_CANCELLED', 'EXPIRED')
            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
            ),
so_report as (
              select DISTINCT date(creation) as creation,
              modified,
              name,
              sales_partner,
              from erp_so so
              --where index = 1
              where name in ('SAL-ORD-ATHEEX9')
              )
select * from so_report