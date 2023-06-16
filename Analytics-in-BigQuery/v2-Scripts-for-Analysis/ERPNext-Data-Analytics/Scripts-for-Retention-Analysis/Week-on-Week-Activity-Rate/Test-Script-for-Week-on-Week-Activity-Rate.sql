--------------------- Test Script ------------------------------
--------------------- Created By Rodgers ------------------------
with
paid_and_delivered_delivery_notes as (
                                      SELECT distinct date_trunc(posting_date, week) as posting_week,
                                      customer
                                      FROM `kyosk-prod.erp_scheduled_queries.erp_paid_and_delivered_dns` 
                                      where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                                      and territory = 'Athi River'
                                      ),
previous_week_base as (
                        select distinct customer
                        from paid_and_delivered_delivery_notes
                        where posting_week <= '2023-04-15'
                        ),
current_week_customer_list as (
                                select distinct customer
                                from paid_and_delivered_delivery_notes
                                where posting_week = '2023-04-16'
                                )
select * from current_week_customer_list where customer in (select distinct customer from previous_week_base)