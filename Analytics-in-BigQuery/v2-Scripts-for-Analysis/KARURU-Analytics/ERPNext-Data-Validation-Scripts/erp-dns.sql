----------------------- ERPNext ------------
----------------------DNs --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            --where date(creation) >= '2022-02-01'
            where date(creation) between '2022-02-01' and '2023-11-08'
            --where  territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            
            ),
dns_summary as (
                select distinct date(creation) as creation,
                dn.territory,
                customer,
                dn.name,
                from erp_dns dn
                where index = 1
                )
select count(distinct name)
from dns_summary