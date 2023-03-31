---------------------- QA Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              --WHERE payment_type = "Receive"
                              --and company  'KYOSK DIGITAL SERVICES LTD (KE)'
                              --and DATE(creation) between '2023-02-06' and '2023-02-19'
                              --and name in ('ACC-PAY-2023-192824', 'PAY-KARA-2023-15588')
                              where posting_date between '2023-01-01' and '2023-01-31'
                              ),
payment_entry_summary as (
                          select distinct posting_date,
                          count(distinct name)
                          from payment_entry_with_index where index = 1
                          group by 1 order by 1
                          ),                              
payment_entry_lists as (
                          select distinct name
                          --count(distinct name)
                          from payment_entry_with_index where index = 1
                          )

select * from payment_entry_summary 