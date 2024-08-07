----------------------------- Delivery Notes, Fulfilment Centers ------------------
with
------------------- Delivery Notes ------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                --where date(created_at) > date_sub(current_date, interval 1 month)
                where date(created_at) >= date_sub(date_trunc(current_date,month), interval 2 day)
                --where date(created_at) between '2024-01-01' and '2024-07-21'
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                      select distinct date(created_at) as created_date,
                      --created_at,
                      --updated_at,
                      --bq_upload_time,
                      --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                      country_code,
                      territory_id,
                      route_id,
                      --route_name,
                      outlet_id,
                      id,
                      --code,
                      --dn.sale_order_id,
                      dn.status,
                      --delivery_trip_id,
                      --payment_request_id,
                      --agent_name as market_developer,
                      --outlet.phone_number,
                      
                      --outlet.name as outlet_name,
                      --outlet.outlet_code as outlet_code,
                      cast(outlet.latitude as float64) as latitude,
                      cast(outlet.longitude as float64) as longitude,
                      outlet_coordinates[SAFE_OFFSET(0)] as outlet_coordinates_latiude,
                      outlet_coordinates[SAFE_OFFSET(1)] as outlet_coordinates_longitude,
                      
                      cast(delivery_coordinates[SAFE_OFFSET(0)] as float64) as delivery_coordinates_latitude,
                      cast(delivery_coordinates[SAFE_OFFSET(1)] as float64) as delivery_coordinates_longitude,
                      from delivery_notes dn
                      where index = 1
                      ),
------------------- Fulfiment Center ----------------
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
                            country_code,
                            cast(location.latitude as float64) as latitude,
                            cast(location.longitude as float64) as longitude
                            from fulfillment_center
                            where index =1 
                            ),
----------------------------- Reports -----------
mashup as (
            select distinct dn.created_date as dn_created_date,
            dn.country_code,
            dn.territory_id,
            fc.latitude as fulfilment_center_latitude,
            fc.longitude as fulfilment_center_longitude,
            dn.outlet_id,
            dn.latitude as outlet_latitude,
            dn.longitude as outlet_longitude,
            dn.id,
            dn.status,
            dn.delivery_coordinates_latitude,
            dn.delivery_coordinates_longitude,
            round(st_distance(ST_GEOGPOINT(fc.longitude, fc.latitude), ST_GEOGPOINT(dn.longitude, dn.latitude)),2) / 1000 as outlet_registration_distance,
            round(st_distance(ST_GEOGPOINT(fc.longitude, fc.latitude), ST_GEOGPOINT(dn.delivery_coordinates_longitude, dn.delivery_coordinates_latitude)),2) / 1000 as delivery_note_distance,
            abs(round(st_distance(ST_GEOGPOINT(dn.longitude, dn.latitude), ST_GEOGPOINT(dn.delivery_coordinates_longitude, dn.delivery_coordinates_latitude)) / 1000,2)) as outlet_registration_to_delivery_distance
            from delivery_notes_cte dn
            left join fulfillment_center_cte fc on dn.territory_id = fc.name
            ),
report_with_zones as (
                        select *,
                        abs(outlet_registration_distance - delivery_note_distance) as distance_variance,
                        case
                          when delivery_note_distance >= 101 then 'Outer Zone'
                          when delivery_note_distance > 61 then 'Middle Zone'
                          when delivery_note_distance >= 0 then 'Inner Zone' 
                        else null end  as delivery_note_zone,
                        case
                          when outlet_registration_distance >= 101 then 'Outer Zone'
                          when outlet_registration_distance > 61 then 'Middle Zone'
                          when outlet_registration_distance >= 0 then 'Inner Zone' 
                        else null end  as outlet_registered_zone ,
                        case
                          when (outlet_registration_to_delivery_distance between 0 and 0.02) then '20m Location Accuracy'
                          when (outlet_registration_to_delivery_distance > 0.02) then '>20m Location Accuracy'
                        else 'UNRECOGNIZED' end as location_accuracy
                        from mashup
                        )
select *
from report_with_zones
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
and country_code = 'KE'
and status IN ('PAID','DELIVERED','CASH_COLLECTED')
and delivery_coordinates_latitude is not null
and FORMAT_DATE('%Y%m%d', dn_created_date) between @DS_START_DATE and @DS_END_DATE