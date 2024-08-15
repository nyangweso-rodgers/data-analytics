with
-------------------------------------- Vehicle & Service Providers ----------------
vehicle_v2 as (
            SELECT *,
            row_number()over(partition by id order by updated_at desc) as index
            FROM `kyosk-prod.karuru_reports.vehicle-v2` 
            --WHERE territory_id not in ("Kyosk HQ","Kyosk TZ HQ","Test FC","Test Fresh TZ Territory","Test KE Territory","Test NG Territory","Test TZ Territory","Test UG Territory","Test254") 
            where date(created_at) >= '2021-10-01'
            ),
vehicle_v2_cte as (
                  select distinct created_at,
                  updated_at,
                  bq_upload_time,
                  audit.created_by,
                  audit.modified_by,
                  territory_id,
                  id,
                  legacy_id,
                  license_plate,
                  type,
                  make,
                  volume,
                  load_capacity,
                  case when service_provider_id = '' then null else service_provider_id end as service_provider_id,
                  --fuel_type
                  on_trip,
                  color
                  from vehicle_v2
                  where index = 1
                  ),
---------------------------- Service Provider --------------------------
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
-------------------- Mashup -------------------------------
vehicles_v2_with_service_provider as (
                                      select v.*,
                                      sp.name as vehicle_service_provider_name,
                                      sp.country_code
                                      from vehicle_v2_cte v
                                      left join service_provider_cte sp on v.service_provider_id = sp.id
                                      )
select *
from vehicles_v2_with_service_provider
where license_plate like "K%"
--where id = '0D6GER24PD9RY'
--order by updated_at desc
--order by 1,2