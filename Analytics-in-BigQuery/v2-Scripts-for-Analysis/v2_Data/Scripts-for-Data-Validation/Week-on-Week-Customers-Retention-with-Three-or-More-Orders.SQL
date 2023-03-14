----------------- QA - Retention, 3+ Orders ----------------
with delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            ),
previous_week as (
                  select distinct customer, count(distinct posting_date) as count_of_posting_dates
                  from delivery_note_with_index 
                  where index = 1 and date_trunc(posting_date,week) = '2023-01-29' 
                  group by 1
                  having count_of_posting_dates >= 3
                  ),
current_week as (
                  select distinct customer, count(distinct posting_date) as count_of_posting_dates
                  from delivery_note_with_index 
                  where index = 1 and date_trunc(posting_date,week) = '2023-02-05'
                  group by 1
                  having count_of_posting_dates >= 3
                  ),
retention as (select count(distinct cw.customer) from current_week cw where cw.customer in (select distinct customer from previous_week))

select * from previous_week