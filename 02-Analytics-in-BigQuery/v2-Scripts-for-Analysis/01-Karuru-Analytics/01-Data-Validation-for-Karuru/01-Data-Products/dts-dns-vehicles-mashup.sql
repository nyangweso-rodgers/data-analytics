-------------- Karuru ---------------
-------------- DTs ---------------------
with
-------------------------- DTs ---------------------------------
karuru_dts as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                where date(created_at) >= '2023-08-07'
                --where date(created_at) <= '2023-11-08'
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and is_pre_karuru = false
                and country_code = 'KE'
              ),
dts_with_timestamps as (
                        select distinct date(created_at) as created_at,
                        date(status_change_history.change_time) as completed_date,
                        country_code,
                        territory_id,
                        id,
                        code,
                        status as dt_status,
                        driver.id as driver_id,
                        driver.code as driver_code,
                        driver.name as driver_name
                        from karuru_dts, unnest(status_change_history) status_change_history
                        where index = 1
                        and status_change_history.to_status = 'COMPLETED'
                        and karuru_dts.id = '0EDCYA2MJZWXY'
                        ORDER BY 1 DESC
                      ),
------------------ DNs ------------------
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at) > '2023-08-05'
                ),
dns_list as (
              select distinct dn.delivery_trip_id,
              id,
              code,
              dn.agent_name,
              --dn.sale_order_id,
              dn.status,
              sum(oi.total_delivered) as total_delivered
              from karuru_dns dn, unnest(order_items) oi
              where index = 1
              AND dn.status IN ('PAID', 'DELIVERED')
              and oi.status = 'ITEM_FULFILLED'
              group by 1,2,3,4,5
              ),
------------------ DNs Zones -------------------
dns_zones as (
              select distinct id,
              delivery_zone,
              distance_in_kms
              from `karuru_scheduled_queries.karuru_dns_distance`
              where creation_date >= '2023-01-01'
              ),
--------------------- Vehicle Assignment -----------------
karuru_vehicle_assignment as (
                              SELECT *,
                              row_number()over(partition by id order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.vehicle_assignment` 
                              WHERE date(created_at) >= "2023-01-01"
                              ),
vehicle_assignment as (
                        select distinct id as vehicle_assignment_id,
                        vehicle_id,
                        driver_id,
                        date_assigned,
                        date_unassigned
                        from karuru_vehicle_assignment
                        where index = 1
                        ),
---------------- Vehicle -------------
karuru_vehicle as (
                    SELECT *,
                    row_number()over(partition by id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.vehicle` 
                    WHERE date(created_at) >= '2023-10-01'
                    ),
vehicles as (
              select distinct 
              id,
              license_plate,
              code as vehicle_code,
              vehicle_type_id
              from karuru_vehicle
              where index = 1
              ),
------------------ Vehicle Types -----------
karuru_vehicle_type as (  
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.vehicle_type` 
                        WHERE date(created_at) >='2023-10-13'
                        ),
vehicle_type as (
                  select distinct id,
                  car_type,
                  vehicle_capacity 
                  from karuru_vehicle_type
                  where index = 1
                  ),
------------------ Mashup -----------------
dts_mashup as (
                select dts.*,
                va.vehicle_assignment_id,
                va.vehicle_id,
                va.date_assigned,
                va.date_unassigned,
                v.license_plate,
                v.vehicle_code,
                vt.car_type,
                vt.vehicle_capacity,
                dn.id as dn_id,
                dn.code as dn_code,
                dn.status as dn_status,
                dn.total_delivered,
                dn.agent_name,
                dns_zones.delivery_zone,
                dns_zones.distance_in_kms
                from dts_with_timestamps dts
                left join vehicle_assignment va on dts.driver_id = va.driver_id
                left join vehicles v on va.vehicle_id = v.id
                left join vehicle_type vt on v.vehicle_type_id = vt.id
                left join dns_list dn on dts.id = dn.delivery_trip_id
                left join dns_zones on dn.id = dns_zones.id
                )
select *, 
LAST_VALUE(delivery_zone) OVER (PARTITION BY id ORDER BY distance_in_kms ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_dt_zone
from dts_mashup
--where id = '0EDDC4PK2ZW49'