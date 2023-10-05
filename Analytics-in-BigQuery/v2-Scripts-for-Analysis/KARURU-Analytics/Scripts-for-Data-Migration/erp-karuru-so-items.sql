------------ ERPNext vs. Karuru 
--------------- Sales Orders Items ---------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            where date(creation) between '2022-02-01' and '2023-09-20'
                            and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                            ),
erp_order_items as (
                    select distinct so.name,
                    soi.item_code,
                    soi.item_group
                    from sales_order_with_index so, unnest(items) soi
                    where index = 1
                    ),
karuru_sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name  order by last_modified_date desc) as index
                            FROM `kyosk-prod.karuru_reports.sales_order` so
                            WHERE date(created_date) between '2022-02-01' and '2023-09-20'
                            and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                            ),
karuru_order_items as (
                        select distinct so.name,
                        soi.product_bundle_id,
                        soi.category_id
                        from karuru_sales_order_with_index so, unnest(items) as soi
                        where index = 1
                        ),
mashup as (
            select e.*,
            e.item_group = k.category_id as item_group_check
            from erp_order_items e
            left join karuru_order_items k on e.name = k.name and e.item_code = k.product_bundle_id
            )
select * from mashup
where item_group_check is false