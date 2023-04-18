---------------------- QA Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              WHERE payment_type = "Receive"
                              --and company  'KYOSK DIGITAL SERVICES LTD (KE)'
                              --and name in ('ACC-PAY-2023-192824', 'PAY-KARA-2023-15588')
                              --and posting_date between '2023-04-01' and '2023-04-17'
                              --and posting_date in ('2023-03-04', '2023-03-05', '2023-03-11', '2023-03-14')
                              ),
payment_entry_summary as (
                          select distinct posting_date,
                          count(distinct name)
                          from payment_entry_with_index where index = 1
                          group by 1 order by 1
                          ),                              
payment_entry_lists as (
                          select distinct name,kyosk_sales_order, paid_amount
                          from payment_entry_with_index where index = 1
                          )

select * from payment_entry_lists 
