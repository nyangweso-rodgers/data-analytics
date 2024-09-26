
-------------- DTs and DNs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --where date(created_at) = current_date
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 week)
                and date(created_at) between '2024-05-01' and '2024-06-30'
                --and is_pre_karuru = false
                and country_code = 'KE'
                and status not in ('CANCELLED')
              ),
delivery_trips_cte as (
                      select distinct
                      created_at,
                      --updated_at,
                      --bq_upload_time,
                      country_code,
                      territory_id,
                      id,
                      code,
                      status,
                      vehicle.id as vehicle_id,
                      vehicle.licence_plate,
                      vehicle.vehicle_type,
                      delivery_note_ids as delivery_note_id,
                      --driver.id as driver_id,
                      --driver.code as driver_code,
                      --driver.name as driver_name,
                      --vehicle_id,
                      --service_provider.id as service_provider_id,
                      --service_provider.name as service_provider_name
                      
                      from delivery_trips, unnest(delivery_note_ids) delivery_note_ids
                      where index = 1
                    ),
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and date(created_at) between '2024-04-01' and '2024-07-31'
                --date(created_date) >= date_sub(date_trunc(current_date(), month), interval 1 month)
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 week)
                --where date(created_at) > date_sub(current_date, interval 30 day)
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                          select distinct date(created_at) as created_at,
                          coalesce(delivery_date, updated_at) as delivery_date,
                          case 
                            when dn.country_code in ('TZ','KE','UG') then date_add(delivery_date, interval 3 hour)
                            when dn.country_code in ('NG') then date_add(delivery_date, interval 2 hour)
                          else dn.delivery_date end as delivery_date_in_local,
                          dn.delivery_window.delivery_date as scheduled_delivery_date,
                          cast(dn.delivery_window.start_time as int64) as delivery_window_start_time,
                          cast(dn.delivery_window.end_time as int64) as delivery_window_end_time,
                          case
                            when (dn.delivery_window.start_time = "8") and (dn.delivery_window.end_time = "14") then '8-14 Delivery Window'
                            when (dn.delivery_window.start_time = "13") and (dn.delivery_window.end_time = "19") then '13-19 Delivery Window'
                          else 'UNSET' end as delivey_window_name,
                          --route_id,
                          delivery_trip_id,
                          id,
                          code,
                          --dn.sale_order_id,
                          dn.status,
                          --payment_request_id,
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          --outlet_id,
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          --outlet.latitude,
                          --outlet.longitude,
                          --outlet_coordinates[OFFSET(0)] as outlet_coordinates_latiude,
                          --outlet_coordinates[OFFSET(1)] as outlet_coordinates_longitude,
                          sum(case when dn.status in ('CASH_COLLECTED','DELIVERED', 'PAID') and oi.status in ('ITEM_FULFILLED') then oi.total_delivered else 0 end) as gmv_vat_incl,
                          from delivery_notes dn, unnest(order_items) oi
                          where index = 1
                          --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          --and dni.status = 'ITEM_FULFILLED'
                          group by 1,2,3,4,5,6,7,8,9,10,11
                          ),
vehicle as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle` 
            WHERE date(created_at) >= '2023-10-01'
            ),
vehicle_cte as (
              select distinct
              id,
              license_plate,
             -- code,
              --vehicle_type_id
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
delivery_trip_and_delivery_notes_mashup as (
                                            select distinct date(dt.created_at) as delivery_trip_creation_date,
                                            dt.country_code,
                                            dt.territory_id,
                                            dt.id as delivery_trip_id,
                                            --dt.code as dt_code,
                                            dt.status as delivery_trip_status,
                                            --dt.vehicle_id,
                                            dt.delivery_note_id,
                                            date(dn.created_at) as delivery_note_creatio_date,
                                            dn.code as delivery_note_code,
                                            dn.status as delivery_note_status,
                                            dn.scheduled_delivery_date as delivery_note_scheduled_delvery_date,
                                            dn.delivey_window_name,
                                            dn.delivery_window_start_time,
                                            delivery_window_end_time,
                                            dn.delivery_date as delivery_note_delivery_date,
                                            dn.delivery_date_in_local as delivery_note_delivery_date_in_local,
                                            EXTRACT(HOUR FROM dn.delivery_date_in_local) as delivery_hour,
                                            --dn.outlet_id,
                                            --dn.latitude as outlet_latitude,
                                            --dn.longitude as outlet_longitude,
                                            --dn.outlet_coordinates_latiude,
                                            --dn.outlet_coordinates_longitude
                                            --dn.status as dn_status,
                                            --v.license_plate
                                            dn.gmv_vat_incl
                                            from delivery_trips_cte dt
                                            left join delivery_notes_cte dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                                            --left join vehicle_cte v on dt.vehicle_id = v.id
                                            --where dn.status not in ('RESCHEDULED', 'DRIVER_CANCELLED')
                                            ),
weekly_deliveries_cte as (
                            select distinct country_code,
                            date_trunc(delivery_trip_creation_date, week) as delivery_trip_week,
                            count(delivery_trip_id) as delivery_trip_count,
                            sum(gmv_vat_incl) as gmv_vat_incl,
                            sum(case when delivery_hour between delivery_window_start_time and delivery_window_end_time then gmv_vat_incl else 0 end) as on_time_gmv,
                            sum(case when delivery_hour not between delivery_window_start_time and delivery_window_end_time then gmv_vat_incl else 0 end) as early_or_late_gmv
                            from delivery_trip_and_delivery_notes_mashup
                            group by 1,2
                            order by 1
                            )
/*
get_outlet_locations as (
                          select distinct country_code,
                          outlet_id,
                          last_value(territory_id)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_territory_id,
                          first_value(outlet_latitude)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as earliest_outlet_latitude,
                          first_value(outlet_longitude)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as earliest_outlet_longitude,
                          last_value(outlet_latitude)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_outlet_latitude,
                          last_value(outlet_longitude)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_outlet_longitude,
                          last_value(outlet_coordinates_latiude)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_outlet_coordinates_latiude,
                          last_value(outlet_coordinates_longitude)over(partition by outlet_id order by dt_creation_date asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_outlet_coordinates_longitude,
                          from delivery_trip_and_delivery_notes_mashup
                          )
*/
select distinct *
--sum(case when delivery_window_name = '13-19 Delivery Window' then gmv_vat_incl else 0 end) as gmv_ 

from weekly_deliveries_cte
--select distinct delivery_window_start_time, delivery_window_end_time from delivery_trip_and_delivery_notes_mashup order by 1,2
--max(created_at), max(updated_at), max(bq_upload_time)
--from delivery_trip_and_delivery_notes_mashup