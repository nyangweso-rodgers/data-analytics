------------ Delivery Notes ----------
with
dns as (
        SELECT *,
        row_number()over(partition by id order by updated_at desc) as index
        FROM `kyosk-prod.karuru_reports.delivery_notes` dn
        --where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
        --where territory_id in ('Voi')
        where date(created_at) > '2022-02-01' #start date
        --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
        and status in ('PAID','DELIVERED','CASH_COLLECTED')
        --and date(created_at) = '2024-04-16' 
        --and code = 'DN-NLKL-0HHPPMEK11X2N'
        --and id = '0HJQB8SV7DPQT'
        --and id = '0HJQB8SV7DPQT'
        --and id = '0HNX673RF0YEK'
        --and code = 'DN-KHETIA -EIKT-0HNE8EYHB4127'
        --and code like 'DN-KHETIA%'
        --and fullfilment_center_id = '0HEHY3146QXKF'
        --and code = 'DN-RUIR-0HEYM9WVMYX4D'
        and outlet_id in ('0CWFVFAFNWVFJ', '0CWRTG5N1CTJJ', '0CW7M5CNN7HZ3', '0CW7KGC6YJQQJ')
        ),
dns_status_change_history_cte as (
                        select distinct dn.id,
                        sch.user_id as user_id,
                        sch.from_status as from_status,
                        sch.to_status as to_status,
                        sch.change_time as change_time
                        --dn.status_change_history,
                        from dns dn, unnest(status_change_history) sch
                        where index = 1
                        ),
dns_cte as (
            select distinct dns.created_at,
            --date(created_at) as dn_created_at,
            dns.delivery_window_id,
            dns.delivery_window.id as dn_delivery_window_id,
            dns.delivery_window.delivery_date  as dn_delivery_window_delivery_date,
            dns.delivery_window.start_time as dn_delivery_window_start_time,
            dns.delivery_window.end_time as dn_delivery_window_end_time,
            coalesce(dns.scheduled_delivery_date, dns.delivery_window.delivery_date) as scheduled_delivery_date,

            d.change_time as dispatched_datetime,
            oc.change_time as ops_cancelled_datetime,
            uc.change_time as user_cancelled_datetime,

            dns.country_code,
            dns.territory_id,
            dns.route_id,
            dns.route_name,

            dns.outlet_id,
            dns.outlet.phone_number as outlet_phone_number,
            dns.outlet.name as outlet_name,
            
            dns.delivery_date,

            dns.delivery_trip_id,
            dns.id,
            dns.code,
            dns.status,
            --dns.payment_request_id,

            dns.agent_name as market_developer
            
            from dns
            left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'DISPATCHED') d on dns.id = d.id
            left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'OPS_CANCELLED') oc on dns.id = oc.id
            left join (select distinct id, change_time from dns_status_change_history_cte where to_status = 'USER_CANCELLED') uc on dns.id = uc.id
            --and s.status not in ('INITIATED')
            where index = 1
            ),
dns_outlets_agg_cte as (
                        select distinct date(delivery_date) as delivery_date,
                        country_code,
                        territory_id,
                        route_id,
                        route_name,
                        outlet_id,
                        outlet_name,
                        outlet_phone_number,
                        id,
                        market_developer
                        from dns_cte
                        order by outlet_id, delivery_date
                        )

--select distinct from_status, to_status from dns_status_change_history_cte order by 1,2
select * from dns_outlets_agg_cte dn
--where scheduled_delivery_date = '2024-10-19'
--where scheduled_delivery_date is null
--where country_code = 'KE'
--order by delivery_date desc
--where FORMAT_DATE('%Y%m%d', delivery_date) between @DS_START_DATE and @DS_END_DATE