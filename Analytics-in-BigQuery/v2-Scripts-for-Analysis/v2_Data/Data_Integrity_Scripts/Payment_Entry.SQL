------------------------ Payment Entry ------------------
with
payment_entry_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.payment_entry`
                            where payment_type = 'Receive'
                            --and posting_date in ( '2022-09-01','2022-09-02','2022-09-05','2022-09-09','2022-09-26','2022-09-27','2022-09-28','2022-09-29', '2022-09-30')
                            ),
payment_entry_summary as (select distinct posting_date, count(distinct name) as count_of_payment_entries from payment_entry_with_index where index = 1 group by 1 order by 1),
payment_entry_list as (select distinct posting_date, name, paid_amount  from payment_entry_with_index where index = 1 order by 1,2)

select count(*) from payment_entry_list 