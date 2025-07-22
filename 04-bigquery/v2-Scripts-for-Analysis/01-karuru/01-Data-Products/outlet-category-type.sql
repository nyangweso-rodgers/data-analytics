-------------------- Outlet Category Typ ------------------------
with
outlet_category_type as (
                          SELECT *,
                          row_number()over(partition by id order by updated_at desc) as index 
                          FROM `kyosk-prod.karuru_reports.outlet_category_type` 
                          WHERE date(created_at) >= '2022-02-01'
                          and outlet_id = '0DVA9SD3AHYGD'
                          ),
outlet_category_type_cte as (
                            select created_at,
                            updated_at,
                            id,
                            outlet_id,
                            case when outlet_type_id = '' then null else outlet_type_id end as outlet_type_id,
                            active_category,
                            outlet_group,
                            case when kyosk_category = '' then null else kyosk_category end as kyosk_category
                            from outlet_category_type
                            where index =1 
                            ),
outlet_category_type_agg_cte as (
                                select distinct kyosk_category, 
                                count(distinct id)
                                from outlet_category_type_cte
                                group by 1
                                order by 2 desc
                                )
select *
--distinct outlet_id, count(distinct outlet_group) as outlet_group
from outlet_category_type_cte