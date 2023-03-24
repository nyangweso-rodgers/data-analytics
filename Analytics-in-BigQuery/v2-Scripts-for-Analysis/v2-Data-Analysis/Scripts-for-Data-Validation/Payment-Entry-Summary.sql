---------------------- QA Payment Entry --------------------------
with
payment_entry_with_index as (
                              SELECT *, row_number()over(partition by name order by modified desc) as index
                              FROM `kyosk-prod.erp_reports.payment_entry` 
                              WHERE payment_type = "Receive"
                              --and DATE(creation) between '2023-02-06' and '2023-02-19'
                              and name in ('ACC-PAY-2022-1555336')
                              ),
payment_entry_summary as (select distinct name from payment_entry_with_index where index = 1)

select * from payment_entry_summary 