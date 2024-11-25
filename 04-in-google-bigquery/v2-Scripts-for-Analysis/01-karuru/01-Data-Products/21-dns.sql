------------ Delivery Notes ----------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --where territory_id in ('Voi')
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
                --and date(created_at) = '2024-04-16' 
                --and code = 'DN-NLKL-0HHPPMEK11X2N'
                --and id = '0HJQB8SV7DPQT'
                --and id = '0HJQB8SV7DPQT'
                --and status in ('PAID')
                --and id = '0HNX673RF0YEK'
                --and code = 'DN-KHETIA -EIKT-0HNE8EYHB4127'
                --and code like 'DN-KHETIA%'
                --and fullfilment_center_id = '0HEHY3146QXKF'
                ),
dns_status_change_history_cte as (
                        select distinct dn.id,
                        sch.user_id as user_id,
                        sch.from_status as from_status,
                        sch.to_status as to_status,
                        sch.change_time as change_time
                        --dn.status_change_history,
                        from delivery_notes dn, unnest(status_change_history) sch
                        where index = 1
                        ),
delivery_notes_cte as (
                        select distinct created_at,
                        --date(created_at) as dn_created_at,
                        dn.delivery_window_id,
                        dn.delivery_window.delivery_date  as delivery_window_delivery_date,
                        dn.scheduled_delivery_date,

                        d.change_time as dispatched_datetime,
                        oc.change_time as ops_cancelled_datetime,
                        uc.change_time as user_cancelled_datetime,

                        dn.country_code,
                        dn.territory_id,
                        
                        dn.delivery_date,

                        dn.delivery_trip_id,
                        dn.id,
                        dn.code,
                        dn.status,
                        dn.payment_request_id,
                        dn.outlet_id,
                        outlet.name as outlet_name,
                        outlet.phone_number as outlet_phone_number,
                        from delivery_notes dn
                        left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'DISPATCHED') d on dn.id = d.id
                        left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'OPS_CANCELLED') oc on dn.id = oc.id
                        left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'USER_CANCELLED') uc on dn.id = uc.id
                        --and s.status not in ('INITIATED')
                        where index = 1
                        )

--select distinct from_status, to_status from dns_status_change_history_cte order by 1,2
select distinct code, status  from delivery_notes_cte dn
--where scheduled_delivery_date = '2024-10-19'
--where scheduled_delivery_date is null
--where country_code = 'KE'
--order by delivery_date desc
where code = "DN-IGOM-0HTYDNY2NNEA3"