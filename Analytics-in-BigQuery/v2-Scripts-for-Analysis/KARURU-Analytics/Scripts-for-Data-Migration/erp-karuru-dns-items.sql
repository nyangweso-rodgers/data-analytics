---------------- erp vs. karuru -----------
--------------- Monthly DNs items ------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at) >= '2022-02-01'
                ),
karuru_dns_items as (
                      select distinct date(delivery_date) as delivery_date,
                      code, 
                      dn.status as dn_status,
                      --oi.item_group_id,
                      oi.product_bundle_id,
                      oi.uom,
                      oi.status as order_item_status,
                      --is_pre_karuru,
                      --total_orderd,
                      round(sum(total_delivered)) as total_delivered
                      from karuru_dns dn, unnest(order_items) oi
                      where index = 1 
                      and is_pre_karuru = true
                      and dn.status in ('PAID', 'DELIVERED')
                      and oi.status = 'ITEM_FULFILLED'
                      group by 1,2,3,4,5,6
                      ),
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_dns_items as (
                  select distinct company,
                  date(dn.posting_date) as posting_date,
                  dn.name, 
                  dn.workflow_state,
                  --dni.item_group,
                  dni.item_code,
                  dni.uom,
                  --concat('ITEM_',dni.fulfilment_status) as fulfilment_status,
                  round(sum(dni.base_amount)) as base_amount,
                  from erp_dns dn, unnest(items) dni 
                  where index = 1
                  and workflow_state in ('PAID', 'DELIVERED')
                  and posting_date between '2022-02-01' and '2023-08-31'
                  --and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                  group by 1,2,3,4,5,6
                  ),
erp_karuru_items_mashup as (
                            select e.*, k.*,
                            posting_date = delivery_date as date_check,
                            item_code = product_bundle_id as item_check,
                            --item_group = item_group_id as category_check,
                            --fulfilment_status = order_item_status as item_status_check
                            base_amount = total_delivered as revenue_check
                            from erp_dns_items e
                            left join karuru_dns_items k on e.name = k.code and e.item_code = k.product_bundle_id and e.uom = k.uom
                            where e.name not in (SELECT distinct name FROM `kyosk-prod.karuru_reports.regenerated_dns` )
                            --order by 1,2,5,6
                            ),
monthly_agg as (
                select distinct company,
                date_trunc(posting_date, month) as posting_month,
                count(distinct name) as erp_name,
                count(distinct code) as karuru_code,
                count(distinct code) - count(distinct name) as dns_variance,
                sum(base_amount) as erp_base_amount,
                sum(total_delivered) as total_delivered,
                sum(total_delivered) - sum(base_amount) as revenue_var
                from erp_karuru_items_mashup e
                group by 1,2
                order by 1,2
                )

select *
from erp_karuru_items_mashup
where product_bundle_id is null