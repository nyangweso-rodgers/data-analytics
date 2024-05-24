
-------------- DTs & DNs ---------------------
with
delivery_trips as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                --where date(created_at) between '2023-08-01' and '2024-01-23'
                --and is_pre_karuru = false
              ),
delivery_trips_report as (
                          select distinct date(created_at) as created_at,
                          country_code,
                          territory_id,
                          id,
                          code,
                          status,
                          --driver.id as driver_id,
                          --driver.code as driver_code,
                          --driver.name as driver_name,
                          --vehicle_id,
                          --service_provider.id as service_provider_id,
                          --service_provider.name as service_provider_name
                          delivery_note_ids as delivery_note_id
                          from delivery_trips, unnest(delivery_note_ids) delivery_note_ids
                          where index = 1
                          and status not in ('CANCELLED')
                        ),
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date(created_at) = current_date
                where date(created_at) > date_sub(current_date, interval 30 day)
                ),
delivery_notes_report as (
                          select distinct --date(created_at) as 
                          created_at,
                          --updated_at,
                          --coalesce(date(delivery_date), date(updated_at)) as delivery_date,
                          --country_code,
                          territory_id,
                          --route_id,
                          --route_name,
                          id,
                          code,
                          --dn.sale_order_id,
                          dn.status,
                          delivery_trip_id,
                          --payment_request_id,
                          --agent_name as market_developer,
                          --outlet.phone_number,
                          --outlet_id,
                          --outlet.name as outlet_name,
                          --outlet.outlet_code as outlet_code,
                          --outlet.latitude,
                          --outlet.longitude,
                          --route_id,
                          from delivery_notes dn
                          where index = 1
                          --and country_code = 'TZ'
                          --and territory_id in ('Vingunguti')
                          --AND dn.status IN ('PAID','DELIVERED','CASH_COLLECTED')
                          --and dni.status = 'ITEM_FULFILLED'
                          ),
dt_and_dn_report as (
                      select dt.*,
                      dn.code as dn_code,
                      dn.status as dn_status
                      from delivery_trips_report dt
                      left join delivery_notes_report dn on dt.id = dn.delivery_trip_id and dt.delivery_note_id = dn.id 
                      )
select *
from dt_and_dn_report
where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
and id = '0FWMQR0X6QQQT'
and dn_code = 'DN-KARA-0FWK97MDPQNST'