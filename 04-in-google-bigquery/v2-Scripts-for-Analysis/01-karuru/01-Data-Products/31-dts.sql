
-------------- DTs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2024-01-01'
                --where date(created_at) = current_date
                --and date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
                --and date(created_at) between '2024-05-01' and '2024-06-31'
                --and is_pre_karuru = false
                --and code like "DT-KHETIA%"
                --and country_code = 'KE'
                --and territory_id = 'Ruiru'
                --and id = '0HQTDQ89G1W3T'
                --and fulfillment_center_id = '0HEHY3146QXKF'
                --AND id = '0HP7MVP470X3J'
                and fulfillment_center_id = '0HNP721RAVN7E'
              ),
dt_status_change_history_cte as (    
                                  select distinct dt.id,
                                  sch.from_status,
                                  sch.to_status,
                                  sch.change_time,
                                  --case when sch.to_status = 'COMPLETED' then sch.change_time end as completed_time
                                  --max(case when sch.to_status = 'COMPLETED' then sch.change_time end)  as completed_time,
                                  --max(case when sch.to_status = 'DISPATCHED' then sch.change_time end)  as dispatched_time,
                                  --max(case when sch.to_status = 'DELIVERED' then sch.change_time end)  as delivered_time
                                  from delivery_trips dt,unnest(status_change_history) sch
                                  --where index = 1 
                                  --group by 1
                                  ),
dts_cte as (
            select distinct created_at,
            updated_at,
            bq_upload_time,

            --dtschct.change_time as dt_completed_datetime,

            country_code,
            territory_id,
            fulfillment_center_id,

            warehouse_location.latitude as warehouse_latitude,
            warehouse_location.latitude as warehouse_longitude,

            --warehouse_location_v2.latitude as warehouse_v2_latitude,

            is_preplanned, 
            dispatch_time.dispatch_window_type,

            dt.id,
            code,
            status,
            --vehicle.id as vehicle_id,
            
            --delivery_note_ids as delivery_note_id,
            driver.id as driver_id,
            driver.code as driver_code,
            driver.name as driver_name,
            driver_wh_id,
            dt.driver_provider_id,

            vehicle_id,
            vehicle.licence_plate,
            vehicle.vehicle_type,
            vehicle_v2.id as vehicle_v2_id,
            vehicle_v2.license_plate as vehicle_v2_license_plate,
            vehicle_v2.type as vehicle_v2_type,
            vehicle_v2.load_capacity as vehicle_v2_load_capacity,
            vehicle_v2.volume as vehicle_v2_volume,
            dt.vehicle_provider_id,

            dispatched_value,
            service_provider.id as service_provider_id,
            service_provider.name as service_provider_name,
            
            from delivery_trips dt, unnest(delivery_note_ids) delivery_note_ids
            --left join (select distinct id, change_time from dt_status_change_history_cte where to_status = 'COMPLETED') dtschct on dt.id = dtschct.id
            where index = 1
          )
--select distinct created_at, updated_at, bq_upload_time,id, code, status from dts_cte
--distinct country_code, count(distinct id)
--select min(created_at) as min_created_at_datetime from dts_cte
--select max(created_at) as max_created_at_datetime, max(updated_at) as max_updated_at_datetime, max(bq_upload_time) as max_bq_upload_time_datetime from dts_cte
select * from dts_cte