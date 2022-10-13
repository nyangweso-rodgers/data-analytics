------------------ PAID DNs -----------------------
with
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            --where workflow_state in ('PAID')
                            --where posting_date between '2022-09-01' and '2022-09-30'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
delivery_note_summary as (select distinct posting_date, count(distinct name) as count_of_delivery_notes from delivery_note_with_index where index = 1 group by 1 order by 1),

delivery_note_lists as (select distinct posting_date, name, workflow_state, grand_total from delivery_note_with_index where index = 1 order by 1,2)



select distinct date_trunc(posting_date, month) as start_date,last_day(posting_date, month) as end_date, company from delivery_note_with_index where index = 1  order by 1,2