
-------------- DTs and DNs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date(created_at) = current_date
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 week)
                where date(created_at) between '2024-07-01' and '2024-07-15'
                --and is_pre_karuru = false
              ),
delivery_trips_cte as (
                      select distinct date(created_at) as created_at,
                      --created_at,
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
                date(created_date) >= date_sub(date_trunc(current_date(), month), interval 1 month)
                --where date(created_at) = current_date
                where date(created_at) > date_sub(current_date, interval 1 week)
                --where date(created_at) > date_sub(current_date, interval 30 day)
                --and is_pre_karuru = false
                ),
delivery_notes_cte as (
                          select distinct date(created_at) as created_at,
                          --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                          --route_id,
                          delivery_trip_id,
                          id,
                          --code,
                          --dn.sale_order_id,
                          --dn.status,
                          --payment_request_id,
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          outlet_id,
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          outlet.latitude,
                          outlet.longitude,
                          outlet_coordinates[OFFSET(0)] as outlet_coordinates_latiude,
                          outlet_coordinates[OFFSET(1)] as outlet_coordinates_longitude,
                          from delivery_notes dn
                          where index = 1
                          --and country_code = 'TZ'
                          --and territory_id in ('Vingunguti')
                          --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          --and dni.status = 'ITEM_FULFILLED'
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
              code,
              vehicle_type_id
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
delivery_trip_and_delivery_notes_mashup as (
                                            select dt.created_at as dt_creation_date,
                                            dt.country_code,
                                            dt.territory_id,
                                            dt.id as dt_id,
                                            --dt.code as dt_code,
                                            --dt.status as dt_status,
                                            --dt.vehicle_id,
                                            dt.delivery_note_id,
                                            --dn.created_at as dn_created_at,
                                            dn.outlet_id,
                                            dn.latitude as outlet_latitude,
                                            dn.longitude as outlet_longitude,
                                            dn.outlet_coordinates_latiude,
                                            dn.outlet_coordinates_longitude
                                            --dn.status as dn_status,
                                            --v.license_plate
                                            from delivery_trips_cte dt
                                            left join delivery_notes_cte dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id
                                            --left join vehicle_cte v on dt.vehicle_id = v.id
                                            ),
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
select *
--max(created_at), max(updated_at), max(bq_upload_time)
--from delivery_trip_and_delivery_notes_mashup
from get_outlet_locations
--where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
where country_code = 'KE'
--and dt.status not in ('CANCELLED')