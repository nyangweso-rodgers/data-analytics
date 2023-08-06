---------------------- ERPNext - QA Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              where payment_type = "Receive"
                              --and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                              --and date(creation) = '2023-07-27'
                              and posting_date between '2023-08-01' and '2023-08-06'
                              ),                            
payment_entry_lists as (
                          select distinct name, paid_amount
                          from payment_entry_with_index where index = 1 order by 1
                          )

select * from payment_entry_lists 