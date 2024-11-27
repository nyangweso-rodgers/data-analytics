--------------- stock entry ------------------
with
stock_entry as (
                SELECT *,
                row_number()over(partition by id order by modified_at desc) as index
                FROM `kyosk-prod.karuru_reports.stock_entry` 
                WHERE TIMESTAMP_TRUNC(created_at, DAY) > TIMESTAMP("2024-11-21")
                ),
stock_entry_cte as (
                    select distinct created_at,
                    modified_at,
                    bq_upload_time,
                    se.posting_date,
                    se.posting_time,

                    
                    company_id,
                    se.territory_id,

                    se.id,
                    se.name,
                    se.delivery_trip_id,
                    se.stock_entry_type,
                    se.source_warehouse_id,
                    se.source_warehouse_type,
                    se.target_warehouse_id,
                    se.target_warehouse_type,

                    se.workflow_state,
                    se.purpose,
                    se.total_incoming_value,
                    se.total_outgoing_value,
                    se.total_amount,

                    i.item_id,
                    i.item_code,
                    i.item_name,
                    i.uom,
                    i.stock_uom,
                    i.item_group_id,
                    i.qty,
                    i.basic_rate,
                    i.source_warehouse_id as items_source_warehouse_id,
                    i.target_warehouse_id as items_target_warehouse_id,

                    se.remarks,
                    se.reason_for_damage,
                    se.reason_for_adjustment
                    from stock_entry se, unnest(items) i
                    where index =1
                    )
select *
--max(created_at) as max_created_at, max(modified_at) as max_modified_at, max(bq_upload_time) as max_bq_upload_time
from stock_entry