----------------------- Material Transfers --------------------
with
material_transfers as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.material_transfers` 
                        --WHERE date(created_at) > "2023-08-03" #start date
                        where date(created_at) > date_sub(current_date, interval 2 month)
                        ),
material_transfers_list as (
                              select distinct 
                              --date(created_at) as created_at,
                              created_at,
                              updated_at,
                              bq_upload_time,
                              id,
                              name,
                              delivery_trip_id,
                              fulfillment_center_id,
                              territory_id,
                              destination_wh_id,
                              origin_wh_id,
                              mt_type,
                              mt_status,
                              skutt.sku,
                              skutt.uom,
                              skutt.qty,
                              skutt.movement_id,
                              from material_transfers mt, unnest(skus_to_transfer) skutt
                              where index = 1
                              ),
fulfillment_center as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        FROM `kyosk-prod.karuru_reports.fulfillment_center` 
                        WHERE date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_list as (
                            select distinct --date(created_at) created_at,
                            id,
                            name,
                            country_code
                            from fulfillment_center
                            where index =1 
                            ),
material_transfers_report as (
                              select mtl.*,
                              fcl.country_code
                              from material_transfers_list mtl
                              left join fulfillment_center_list fcl on mtl.fulfillment_center_id = fcl.id
                              )
select 
--distinct country_code, territory_id
max(created_at) as max_created_at, max(updated_at) as max_last_updated_at, max(bq_upload_time) as max_bq_upload_time
from material_transfers_list
where territory_id not in ('Test UG Territory', 'Test TZ Territory', 'Test KE Territory')
--and FORMAT_DATE('%Y%m%d', created_at) between @DS_START_DATE and @DS_END_DATE