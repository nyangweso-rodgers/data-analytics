----------------------- ERPNext ------------
----------------------DNs --------------
with 
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where  territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            --where date(creation) between '2023-01-01' and '2023-09-02'
            ),
dns_summary as (
                select distinct 
                territory,
                customer,
                name,
                posting_date,
                grand_total
                from erp_dns dn 
                where index = 1
                --and workflow_state in ('PAID', 'DELIVERED')
                --and posting_date between '2023-05-01' and '2023-09-18'
                )
select *
from dns_summary
where customer = 'oliv shop mathare3'
