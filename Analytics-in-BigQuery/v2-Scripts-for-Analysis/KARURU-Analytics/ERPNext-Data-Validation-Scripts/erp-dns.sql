----------------------- ERPNext ------------
----------------------DNs --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where  territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            --and date(creation) between '2023-10-01' and '2023-10-29'
            ),
dns_summary as (
                select distinct 
                dn.territory,
                customer,
                dn.name,
                from erp_dns dn, unnest(items) dni
                where index = 1
                and against_sales_order in ('SAL-ORD-EMBRGNL', 'SAL-ORD-KISWU5H')
                )
select *
from dns_summary