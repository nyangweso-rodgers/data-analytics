---------------------- ERPNext ----------------
------------- Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              where payment_type = "Receive"
                              and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                              --and posting_date between '2023-08-01' and '2023-08-31'
                              --where posting_date >= '2023-07-01'
                              ),                            
payment_entry_lists as (
                          select distinct creation, posting_date,
                          company,
                          name, 
                          paid_amount,
                          dn_id
                          from payment_entry_with_index 
                          where index = 1 
                          )

select *
from payment_entry_lists 
where date(creation) = '2023-09-16'
--where name in ('PAY-WAEF-2023-22504', 'ACC-PAY-2023-339120', 'PAY-ABBW-03NM-2023')