----------- Material Requests Items ---------------
with

material_request as(
                    SELECT *, 
                    row_number()over(partition by id order by date_modified desc) as index
                    FROM `kyosk-prod.karuru_reports.material_request` 
                    WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
                    --where date(date_created) >= '2022-02-01'
                    and material_request_type = 'PURCHASE'
                    --and company_id =  'KYOSK DIGITAL SERVICES LTD (KE)'
                    --and name = 'MAT-MR-2023-20063'
                  ),
material_request_items as (
                            select distinct --date(mr.date_created) as date_created,
                            mr.date_created,
                            mr.date_modified,
                            mr.bq_upload_time,
                            mr.company_id,

                            mr.id,
                            mr.name, 
                            mr.workflow_state,
                            mr.status,
                            mr.transaction_date,
                            mr.scheduled_date,
                            mr.target_warehouse_territory_id as territory_id,
                            --mri.territory_id,
                            mri.item_name,
                            mri.item_name as item_code,
                            mri.item_group as item_group_id,
                            mri.qty,
                            mri.warehouse_id,
                            mri.uom,
                            mri.ordered_qty,
                            mri.received_qty,
                            mri.rate,
                            mri.amount                                
                            from material_request mr, unnest(items) mri
                            where index = 1
                            )                             
select max(date_created), max(date_modified), max(bq_upload_time)
from material_request_items
--where FORMAT_DATE('%Y%m%d', date_created) between @DS_START_DATE and @DS_END_DATE                     