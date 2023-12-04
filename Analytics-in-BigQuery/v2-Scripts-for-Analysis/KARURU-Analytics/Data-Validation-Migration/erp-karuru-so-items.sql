------------ ERPNext vs. Karuru 
--------------- Sales Orders Items ---------------------
with
erp_so as (
            SELECT *,
            row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            where date(creation) between '2022-02-01' and '2023-10-23'
            --where date(creation) between '2023-09-01' and '2023-09-30'
            --where date(creation) = '2023-10-23'
            and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_order_items as (
                    select distinct date(so.creation) as erp_creation,
                    so.name,
                    soi.item_code,
                    soi.item_group,
                    soi.fulfilment_status
                    from erp_so so, unnest(items) soi
                    where index = 1
                    --and so.name  = 'SAL-ORD-RUIZJFP'
                    ),
karuru_so as (
              SELECT *,
              row_number()over(partition by name  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where date(created_date) between '2022-02-01' and '2023-10-23'
              --WHERE date(created_date) between '2023-09-01' and '2023-09-30'
              --where date(created_date) = '2023-10-23'
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
              and is_pre_karuru = true
              ),
karuru_order_items as (
                        select distinct so.name,
                        soi.product_bundle_id,
                        soi.category_id,
                        soi.fulfilment_status
                        from karuru_so so, unnest(items) as soi
                        where index = 1
                        ),
mashup as (
            select e.*,
            k.fulfilment_status as karuru_fulfilment_status,
            e.item_group = k.category_id as check_for_item_group,
            e.fulfilment_status = k.fulfilment_status as check_for_fulfilment_status
            from erp_order_items e
            left join karuru_order_items k on e.name = k.name and e.item_code = k.product_bundle_id
            )
select distinct fulfilment_status, karuru_fulfilment_status
from mashup
where check_for_fulfilment_status is false
--order by erp_creation desc, name, item_code
order by 1,2