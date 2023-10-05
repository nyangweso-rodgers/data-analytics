------------ ERPNext vs. Karuru 
--------------- Sales Orders ---------------------
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
                    date(creation) as creation,
                    sales_partner
                    from sales_order_with_index so
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
                        date(created_date) as created_date,
                        market_developer_name
                        from karuru_sales_order_with_index so
                        where index = 1
                        ),
mashup as (
            select e.*,
            k.created_date,
            k.market_developer_name,
            e.creation = k.created_date as date_check,
            e.sales_partner = k.market_developer_name as md_check
            from erp_order_items e
            left join karuru_order_items k on e.name = k.name
            )
select * from mashup
where md_check is false 
--and name = 'SAL-ORD-KAWHUHA'