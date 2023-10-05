----------------------- ERPNext ------------
----------------------DNs --------------
with 
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            --where date(creation) between '2023-01-01' and '2023-09-02'
                            ),
dns_summary as (
                select distinct 
                territory,
                customer,
                name,
                posting_date,
                --date(modified) as modified_date,
                --date_diff(date(modified), posting_date,  day) as date_diff,
                case
                  when workflow_state = 'CANCELLED' then 'DRIVER_CANCELLED'
                else workflow_state end as workflow_state,
                grand_total
                from delivery_note_with_index dn 
                where index = 1
                and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                --and workflow_state in ('PAID', 'DELIVERED')
                --and posting_date between '2023-05-01' and '2023-09-18'
                )
select *
from dns_summary
where customer = 'oliv shop mathare3'
