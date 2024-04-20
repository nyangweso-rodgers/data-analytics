------------ ERPNext vs. Karuru 
--------------- SO Items ---------------------
with
erp_so as (
            SELECT *,
            row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            where date(creation) between '2022-02-01' and '2023-12-10'
            and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_so_items as (
                select distinct date(so.creation) as erp_creation,
                so.name,
                soi.item_code,
                soi.item_group,
                case
                  when soi.fulfilment_status = 'CANCELLED' then 'ITEM_CANCELLED'
                  --when soi.fulfilment_status = 'CHANGE_REQUESTED' then 'CHANGE_REQUESTED'
                  when soi.fulfilment_status = 'COMPLETED' then 'ITEM_FULFILLED'
                  when soi.fulfilment_status = 'DELIVERED' then 'ITEM_FULFILLED'
                  when soi.fulfilment_status = 'DELIVERING' then 'DELIVERING'
                  when soi.fulfilment_status = 'DISPATCHED' then 'ITEM_DISPATCHED'
                  when soi.fulfilment_status = 'DISPATCHING' then 'DISPATCHING'
                  when soi.fulfilment_status = 'DRIVER_CANCELLED' then 'ITEM_CANCELLED'
                  when soi.fulfilment_status = 'DRIVER_RESCHEDULED' then 'ITEM_RESCHEDULED'
                  when soi.fulfilment_status = 'PAID' then 'ITEM_FULFILLED'
                  when soi.fulfilment_status = 'RESCHEDULED' then 'ITEM_RESCHEDULED'
                  when soi.fulfilment_status = 'SOLD_ON_CREDIT' then 'SOLD_ON_CREDIT'
                  when soi.fulfilment_status = 'SUBMITTED' then 'ITEM_PROCESSING'
                else soi.fulfilment_status end as fulfilment_status
                from erp_so so, unnest(items) soi
                where index = 1
                and so.name not in (SELECT name FROM `kyosk-prod.erp_reports.so_items_with_multiple_fulfilment_status` )
                ),
karuru_so as (
              SELECT *,
              row_number()over(partition by name  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              where date(created_date) between '2022-02-01' and '2023-12-10'
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
              and is_pre_karuru = true
              ),
karuru_so_items as (
                    select distinct so.name,
                    so.bq_upload_time,
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
            e.fulfilment_status = k.fulfilment_status as check_for_fulfilment_status,
            k.bq_upload_time
            from erp_so_items e
            left join karuru_so_items k on e.name = k.name and e.item_code = k.product_bundle_id
            )
select *
from mashup
where (check_for_item_group = false) --or 
--where (check_for_fulfilment_status = false)