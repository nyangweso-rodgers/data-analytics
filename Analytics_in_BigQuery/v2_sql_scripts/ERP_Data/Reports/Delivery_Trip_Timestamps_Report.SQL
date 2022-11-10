---------------- Delivery Trip Timestamps Report ----------------
with
delivery_trip_with_index as (
                              select *,
                              row_number()over(partition by name order by modified desc) as index
                              from `erp_reports.delivery_trip`
                              --where  workflow_state in ('COMPLETED')
                              --and date(completed_time) = '2022-10-24'
                              where company in ('KYOSK DIGITAL SERVICES LTD (KE)')
                            ),
delivery_trip_report as (
                            select distinct company,
                            territory,
                            name as delivery_trip,
                            driver,
                            driver_name,
                            workflow_state,
                            creation as creation_time,
                            case 
                              when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(creation, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(creation, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(creation, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(creation, interval 1 HOUR) 
                            else creation end as creation_time_in_local,
                            dispatched_time,
                            case 
                              when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(dispatched_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(dispatched_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(dispatched_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(dispatched_time, interval 1 HOUR) 
                            else dispatched_time end as dispatched_time_in_local,
                            first_note_paid,
                            case 
                              when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(first_note_paid, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(first_note_paid, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(first_note_paid, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(first_note_paid, interval 1 HOUR) 
                            else first_note_paid end as first_note_paid_in_local,

                            delivered_time,
                            case 
                              when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(delivered_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(delivered_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(delivered_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(delivered_time, interval 1 HOUR) 
                            else delivered_time end as delivered_time_in_local,

                            completed_time,
                            case 
                              when company = 'KYOSK DIGITAL SERVICES LTD (KE)' then TIMESTAMP_ADD(completed_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then TIMESTAMP_ADD(completed_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then TIMESTAMP_ADD(completed_time, interval 3 HOUR) 
                              when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then TIMESTAMP_ADD(completed_time, interval 1 HOUR) 
                            else completed_time end as completed_time_in_local
 
                            from delivery_trip_with_index
                            where index = 1
                            and FORMAT_DATE('%Y%m%d', date(creation)) between @DS_START_DATE and @DS_END_DATE
                            )
select * from delivery_trip_report