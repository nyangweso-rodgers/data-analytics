---------------------- QA Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              WHERE payment_type = "Receive"
                              --and company  'KYOSK DIGITAL SERVICES LTD (KE)'
                              and posting_date between '2023-05-01' and '2023-05-14'
                              ),
payment_entry_summary as (
                          select distinct posting_date,
                          count(distinct name)
                          from payment_entry_with_index where index = 1
                          group by 1 order by 1
                          ),                              
payment_entry_lists as (
                          select distinct name, 
                          paid_amount
                          from payment_entry_with_index where index = 1
                          )

select * from payment_entry_lists 