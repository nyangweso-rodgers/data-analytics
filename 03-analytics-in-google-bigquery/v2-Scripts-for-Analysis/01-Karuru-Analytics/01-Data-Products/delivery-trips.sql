
-------------- DTs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date(created_at) = current_date
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
                --where date(created_at) between '2023-08-01' and '2024-01-23'
                --and is_pre_karuru = false
              ),
delivery_trips_report as (
                          select distinct --date(created_at) as created_at,
                          --created_at,
                          --updated_at,
                          --bq_upload_time,
                          country_code,
                          territory_id,
                          --warehouse_location, -- to be added
                          --driver_provider_id, -- to be added
                          --vehicle_provider_id,-- to be added
                          --is_preplanned, -- to be added
                          dispatch_time.dispatch_window_type,

                          driver_wh_id,
                          fulfillment_center_id,
                          id,
                          code,
                          status,
                          vehicle.id as vehicle_id,
                          vehicle.licence_plate,
                          vehicle.vehicle_type,
                          delivery_note_ids as delivery_note_id,
                          --driver.id as driver_id,
                          --driver.code as driver_code,
                          driver.name as driver_name,
                          --vehicle_id,
                          --service_provider.id as service_provider_id,
                          --service_provider.name as service_provider_name
                          
                          from delivery_trips, unnest(delivery_note_ids) delivery_note_ids
                          where index = 1
                          --and status not in ('CANCELLED')
                        )
select *
--max(created_at), max(updated_at), max(bq_upload_time)
from delivery_trips_report
where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
--and id = '0FWMQR0X6QQQT'
--and delivery_note_id = '0FWK97MDPQNST'
order by vehicle_id