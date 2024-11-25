-------------------- delivery window v3 -----------------
with
delivery_window_v3 as (
                        SELECT *,
                        row_number()over(partition by id order by updated_at desc) as index
                        FROM `kyosk-prod.karuru_reports.delivery_window_v3` 
                        WHERE TIMESTAMP_TRUNC(created_at, DAY) > TIMESTAMP("2024-01-01")
                        ),
delivery_window_v3_cte as (
                            select distinct created_at,
                            updated_at,
                            bq_upload_time,
                            id,
                            delivery_window_config_id,
                            available,
                            route_cluster_id,
                            cut_off_time,
                            start_time,
                            end_time
                            from delivery_window_v3
                            where index = 1
                            )
select * from delivery_window_v3_cte