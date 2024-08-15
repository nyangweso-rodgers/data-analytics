------------------- Karuru ----------
------------- Vehicles ------------
with
vehicle as (
                    SELECT *,
                    row_number()over(partition by id order by updated_at desc) as index
                    FROM `kyosk-prod.karuru_reports.vehicle` 
                    WHERE date(created_at) >= '2023-10-01'
                    ),
vehicle_cte as (
              select distinct
              created_at,
              updated_at,
              bq_upload_time,
              id,
              license_plate,
              code,
              vehicle_type_id,
              driver_id,
              service_provider_id,
              from vehicle
              where index = 1
              --where id = '0D6GEQY6YDCP9'
              ),
------------------- Vehicle Types ------------------
vehicle_type as (  
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index 
                  FROM `kyosk-prod.karuru_reports.vehicle_type` 
                  WHERE date(created_at) >='2022-10-13'
                  ),
vehicle_type_cte as (
                select distinct --date(created_at) as created_at,
                id,
                code,
                car_type,
                vehicle_capacity 
                from vehicle_type
                where index = 1
                ),
-------------------- Service Provider -----------------------
service_provider as (
                      SELECT *,
                      row_number()over(partition by id order by updated_at desc) as index
                      FROM `kyosk-prod.karuru_reports.service_provider` 
                      WHERE date(created_at) > "2021-01-01"
                      
                      ),

service_provider_cte as (
                          select distinct created_at,
                          updated_at,
                          bq_upload_time,
                          country_code,
                          company_code,
                          id,
                          name,
                          owner,
                          provider_type,
                          disabled,
                          is_transporter,
                          supplier_group,
                          from service_provider
                          where index = 1
                          ),       
vehicle_vehicle_type_and_service_provider as (
                                              select distinct v.updated_at as vehicle_last_modified_datetime,
                                              v.id,
                                              v.license_plate,
                                              v.vehicle_type_id,
                                              sp.name as srvice_provider_name,
                                              sp.owner as service_provider_owner,
                                              sp.updated_at as service_provider_last_modified,
                                              vt.car_type,
                                              vt.vehicle_capacity
                                              --max(created_at) as max_created_at, max(updated_at) as max_updated_at, max(bq_upload_time) as max_bq_upload_time
                                              from vehicle_cte v
                                              left join service_provider_cte sp on v.service_provider_id = sp.id
                                              left join vehicle_type_cte vt on v.vehicle_type_id = vt.id
                                              )
select *
from vehicle_vehicle_type_and_service_provider
--where license_plate like "%T463AMS%"
where license_plate = 'KMFR 025A'