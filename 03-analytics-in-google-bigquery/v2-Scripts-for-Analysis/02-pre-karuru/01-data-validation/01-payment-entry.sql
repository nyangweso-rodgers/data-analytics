---------------------- ERPNext ----------------
------------- Payment Entry --------------------------
with
payment_entry as (
                  SELECT *, row_number()over(partition by name order by modified desc) as index
                  FROM `kyosk-prod.erp_reports.payment_entry` 
                  where payment_type = "Receive"
                  and date(creation) between '2022-01-01' and '2022-12-01'
                  and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                  --and posting_date between '2023-09-19' and '2023-09-24'
                  ),                            
payment_entry_cte as (
                      select distinct creation,
                      posting_date,
                      name, 
                      left(name, 4) as tt
                      --paid_amount, 
                      from payment_entry 
                      where index = 1 
                      )

select distinct tt
--max(creation) as max_creation, max(posting_date) as max_posting_date
from payment_entry_cte 