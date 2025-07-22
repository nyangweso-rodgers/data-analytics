--------------------- --------------Karuru --------------------------------------------
--------------------------------OTIF Report -------------------------------------------
-----------------------------Created By - Rodgers -------------------------------------
-----------Last Updated by Jimmy : 2024-02-02 > Included Renamed Territory from Regional Mapping Table
with
-------------------------------Uploaded Tables---------------------------------------
regional_mapping as (
                    select distinct country,
                    region,
                    sub_region,
                    division,
                    original_territory_id, 
                    new_territory_id,
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping` 
                    ),
warehouse_locations  as (
                          select distinct country,
                          territory,
                          case when territory = 'Karatina' then -0.4127367 else warehouse_latitude end as warehouse_latitude,
                          case when territory = 'Karatina' then 36.9522650 else warehouse_longitude end as warehouse_longitude
                          from `kyosk-prod.karuru_upload_tables.uploaded_warehouse_coordinates`
                          ),
------------------- Uploaded - Delivery Windows ---------------------------------------
uploaded_delivery_window_v1 as (
                                SELECT distinct id,
                                safe_cast(start_time as int64) as start_time,
                                safe_cast(end_time as int64) as end_time
                                FROM `kyosk-prod.karuru_upload_tables.delivery_window_v1` 
                                ),
uploaded_delivery_window_v2 as (
                                SELECT distinct id,
                                safe_cast(delivery_window_start_time_hours as int64) as delivery_window_start_time_hours,
                                safe_cast(delivery_window_end_time_hours as int64) as delivery_window_end_time_hours
                                FROM `kyosk-prod.karuru_upload_tables.delivery_window_v2` 
                                ),
uploaded_delivery_window_v3 as (
                                SELECT distinct id,
                                safe_cast(start_time_hours as int64) as start_time_hours,
                                safe_cast(end_time_hours as int64) as end_time_hours
                                --delivery_window_end_time_hours 
                                FROM `kyosk-prod.karuru_upload_tables.delivery_window_v3` 
                                ),
----------------------- delivery window v3 -----------------------------
delivery_window_v3 as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.delivery_window_v3` 
                        WHERE TIMESTAMP_TRUNC(created_at, DAY) > TIMESTAMP("2024-01-01")
                        ),
delivery_window_v3_cte as (
                            select distinct created_at,
                            --updated_at,
                            --bq_upload_time,
                            id,
                            --delivery_window_config_id,
                            available,
                            --route_cluster_id,
                            cut_off_time,
                            start_time,
                            end_time
                            from delivery_window_v3
                            where index = 1
                            ),
---------------------------- Fulfilment Centers --------------------
fulfillment_center as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                        WHERE date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_cte as (
                            select distinct --date(created_at) created_at,
                            id,
                            name,
                            --country_code,
                            location.latitude,
                            location.longitude
                            from fulfillment_center
                            where index =1 
                            ),
----------------------------------------------------------------------------------------
delivery_notes as (
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index
                  FROM `kyosk-prod.karuru_reports.delivery_notes`
                  where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory') 
                  and DATE(created_at) >= date_sub(date_trunc(current_date(), month), interval 2 month)
                  and status in ('PAID','DELIVERED','CASH_COLLECTED')
                  and code = 'DN-LZRA-0HY9G6PWK0MX9'
                  ),
delivery_notes_cte as (
                          select distinct dn.delivery_window_id,
                          coalesce(dn.scheduled_delivery_date, dn.delivery_window.delivery_date) as scheduled_delivery_date,
                          
                          safe_cast(dn.delivery_window.start_time as int64) as delivery_window_start_time,
                          safe_cast(dn.delivery_window.end_time as int64) as delivery_window_end_time,
                          /*case
                            when (CHAR_LENGTH(dn.delivery_window_id) = 13) then left(right(dn.delivery_window.start_time, 7),1)
                            --when (CHAR_LENGTH(dn.delivery_window_id) > 13) then
                            --when CHAR_LENGTH(dn.delivery_window.start_time) > 2 then left(right(dn.delivery_window.start_time, 7),1)
                          else dn.delivery_window.start_time end as delivery_window_start_time,*/
                          /*case
                            when (CHAR_LENGTH(dn.delivery_window_id) = 13) then left(right(dn.delivery_window.end_time, 8),2)
                            --when (CHAR_LENGTH(dn.delivery_window_id) > 13) then 
                            --when CHAR_LENGTH(dn.delivery_window.start_time) > 2 then left(right(dn.delivery_window.end_time, 8),2)
                          else dn.delivery_window.start_time end as delivery_window_end_time,*/
                          coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                          case 
                            when dn.country_code in ('TZ','KE','UG') then date_add(delivery_date, interval 3 hour)
                            when dn.country_code in ('NG') then date_add(delivery_date, interval 2 hour)
                          else dn.delivery_date end as delivery_date_in_local,
                          dn.country_code,
                          rm.country,
                          rm.region,
                          rm.division,
                          rm.new_territory_id as territory_id,
                          dn.fullfilment_center_id,
                          case when fc.name = "Khetia " then 'Khetia' else rm.new_territory_id  end as fullfilment_center_name,

                          dn.outlet_id,
                          dn.id,
                          dn.code,
                          dn.status,
                          dn.route_name,
                          dn.driver.name as driver_name,
                          cast(dn.outlet.latitude as float64) as duka_latitude,
                          cast(dn.outlet.longitude as float64) as duka_longitude, 
                          wl.warehouse_latitude,
                          wl.warehouse_longitude,
                          from delivery_notes dn
                          left join regional_mapping  rm on dn.territory_id = rm.original_territory_id
                          left join fulfillment_center_cte fc on dn.fullfilment_center_id = fc.id
                          left join warehouse_locations wl on dn.territory_id = wl.territory 
                          where index = 1        
                          ),
------------------------ Delivery Notes , Delivery Windows --------------------------
dns_with_delivery_windows_cte as (
                                    select distinct dn.country_code,
                                    dn.country,
                                    dn.region,
                                    dn.division,
                                    dn.territory_id,
                                    dn.route_name,
                                    
                                    dn.scheduled_delivery_date,
                                    date(delivery_date) as delivery_date,
                                    dn.delivery_date_in_local,
                                    EXTRACT(HOUR FROM delivery_date_in_local) as delivery_hour,

                                    dn.delivery_window_id,
                                    dwv3.available,
                                    case
                                      when dn.country_code in ('TZ','KE','UG') then date_add(dwv3.start_time, interval 3 hour)
                                      when dn.country_code in  ('NG') then date_add(dwv3.start_time, interval 2 hour)
                                    else dwv3.start_time end as delivery_window_v3_local_start_time,
                                    case
                                      when dn.country_code in ('TZ','KE','UG') then date_add(dwv3.end_time, interval 3 hour)
                                      when dn.country_code in  ('NG') then date_add(dwv3.end_time, interval 2 hour)
                                    else dwv3.end_time  end as delivery_window_v3_local_end_time,

                                    --coalesce(dn.delivery_window_start_time, udwv1.start_time, udwv2.delivery_window_start_time_hours,udwv3.start_time_hours)  as delivery_window_start_time,
                                    --coalesce(dn.delivery_window_end_time, udwv1.end_time, udwv2.delivery_window_end_time_hours, udwv3.end_time_hours) as delivery_window_end_time,

                                    udwv1.start_time as uploaded_delivery_window_v1_start_time,
                                    udwv1.end_time as uploaded_delivery_window_v1_end_time,

                                    udwv2.delivery_window_start_time_hours as uploaded_delivery_window_v2_start_time,
                                    udwv2.delivery_window_end_time_hours as uploaded_delivery_window_v2_end_time,

                                    udwv3.start_time_hours as uploaded_delivery_window_v3_start_time,
                                    udwv3.end_time_hours as uploaded_delivery_window_v3_end_time,

                                    
                                    --date(delivery_date_in_local) as delivery_date, 

                                    dn.fullfilment_center_name,
                                    dn.warehouse_latitude,
                                    dn.warehouse_longitude,

                                    dn.duka_latitude,
                                    dn.duka_longitude,

                                    dn.outlet_id,
                                    dn.id,
                                    dn.code,
                                    dn.status,

                                    dn.driver_name,
                                    from delivery_notes_cte dn
                                    left join delivery_window_v3_cte dwv3 on dn.delivery_window_id = dwv3.id
                                    left join uploaded_delivery_window_v1 udwv1 on dn.delivery_window_id = udwv1.id
                                    left join uploaded_delivery_window_v2 udwv2 on dn.delivery_window_id = udwv2.id
                                    left join uploaded_delivery_window_v3 udwv3 on dn.delivery_window_id = udwv3.id
                                    ),
updated_dns_with_delivery_window_v3 as (
                                        select distinct country_code,
                                        country,
                                        region,
                                        division,
                                        territory_id,
                                        route_name,
                                        fullfilment_center_name,
                                        warehouse_latitude,
                                        warehouse_longitude,
                                        round(st_distance(ST_GEOGPOINT(warehouse_longitude,warehouse_latitude), ST_GEOGPOINT(duka_longitude, duka_latitude)),0) / 1000 as distance_in_kms,

                                        scheduled_delivery_date,
                                        delivery_date,
                                        delivery_hour,
                                        delivery_window_id,
                                        delivery_window_v3_local_start_time,
                                        EXTRACT(HOUR FROM delivery_window_v3_local_start_time) as delivery_window_start_hour,
                                        delivery_window_v3_local_end_time,
                                        EXTRACT(HOUR FROM delivery_window_v3_local_end_time) as delivery_window_end_hour,

                                        --delivery_window_start_time
                                        outlet_id,
                                        duka_latitude,
                                        duka_longitude,

                                        id,
                                        code,
                                        status,

                                        driver_name,
                                        from dns_with_delivery_windows_cte
                                        ),
calculate_otif_report_cte as (
                select *,
                case 
                  when (delivery_date = scheduled_delivery_date) and (delivery_hour between delivery_window_start_hour and delivery_window_end_hour) then 'ON-TIME DELIVERY' 
                  when (delivery_date > scheduled_delivery_date) or (delivery_date = scheduled_delivery_date and delivery_hour > delivery_window_end_hour) then 'LATE DELIVERY'
                  when (delivery_date < scheduled_delivery_date) or (delivery_date = scheduled_delivery_date and delivery_hour < delivery_window_start_hour)  then 'EARLY DELIVERY'
                else 'UNSET' end as otif_status,
                case
                  when distance_in_kms >= 101 then 'Outer Zone'
                  when distance_in_kms > 61 then 'Middle Zone'
                  when distance_in_kms >= 0 then 'Inner Zone' 
                else null end  as delivery_zone 
                from updated_dns_with_delivery_window_v3
                )
/*,
-------------------- QA - By Rodgers ------------------
country_agg_cte as (
                select distinct country_code,
                territory_id,
                count(distinct id) as dns_count
                from otif_report_with_zones
                where delivery_date = '2024-10-15'
                group by 1,2
                )*/
select * from calculate_otif_report_cte 
--where delivery_date = '2024-11-19'
--where scheduled_delivery_date = '2024-11-20'
--where FORMAT_DATE('%Y%m%d', delivery_date) between @DS_START_DATE and @DS_END_DATE  