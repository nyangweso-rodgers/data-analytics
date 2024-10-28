-------------- ERP --------
---------- SO vs. DN---------------------
with
erp_so as (
            SELECT *,
            row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            --where date(creation) between '2022-02-01' and  '2023-11-09'
            where date(creation) = '2022-10-21'
            and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            and docstatus = 1
            --and workflow_state not in ('INITIATED')--, 'USER_CANCELLED', 'EXPIRED')
            --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
            ),
so_summary as (
              select DISTINCT date(creation) as creation,
              modified,
              name,
              workflow_state,
              docstatus,
              sales_partner,
              from erp_so so
              --where index = 1
              ),
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            --where date(creation) >= '2022-02-01'
            where date(creation) between '2022-10-21' and '2022-10-30'
            --where  territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            
            ),
dns_summary as (
                select distinct date(creation) as creation,
                --dn.territory,
                customer,
                dn.name,
                dn.kyosk_sales_order,
                dn.workflow_state,
                dn.sales_partner
                from erp_dns dn
                where index = 1
                )
select so.*,
dn.name as delivery_note,
dn.customer as dn_customer,
dn.workflow_state as dn_workflow_state,
dn.sales_partner as dn_sales_partner
from so_summary so
left join dns_summary dn on so.name = dn.kyosk_sales_order
where so.name in ('SAL-ORD-KAW3DT0')
order by name, creation desc