---------------------- ERPNext ----------------
------------- Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              where payment_type = "Receive"
                              --and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                              and posting_date between '2023-09-19' and '2023-09-24'
                              ),                            
payment_entry_lists as (
                          select distinct
                          name, 
                          paid_amount, 
                          from payment_entry_with_index 
                          where index = 1 
                          )

select *
from payment_entry_lists 