---------------- erp vs. karuru -----------
---------------  DNs items ------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
               -- where date(created_at) >= '2022-02-01'
                where date(created_at) between '2022-02-01' and '2022-09-30'
                ),
karuru_dns_items as (
                      select distinct  date(created_at) as created_at,
                      delivery_date,
                      id,
                      code, 
                      dn.status as dn_status,
                      --oi.item_group_id,
                      oi.product_bundle_id,
                      oi.uom,
                      oi.status as order_item_status,
                      --total_orderd,
                      round(sum(total_delivered)) as total_delivered
                      from karuru_dns dn, unnest(order_items) oi
                      where index = 1 
                      and is_pre_karuru = true
                      and dn.status in ('PAID', 'DELIVERED')
                      and oi.status = 'ITEM_FULFILLED'
                      group by 1,2,3,4,5,6,7,8
                      ),
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            where date(creation) between '2022-02-01' and '2022-09-30'
            --where territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_dns_items as (
                  select distinct date(dn.creation) as creation,
                  company,
                  --date(dn.posting_date) as posting_date,
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
                  and dni.item_code not in (SELECT * FROM `kyosk-prod.karuru_reports.dn_items_with_null_uom`)
                  group by 1,2,3,4,5,6
                  ),
erp_vs_karuru_items_mashup as (
                            select e.*, k.*,
                            e.creation = k.created_at as check_created_at,
                            e.uom = k.uom as check_uom,
                            --posting_date = delivery_date as date_check,
                            item_code = product_bundle_id as item_check,
                            --item_group = item_group_id as category_check,
                            --fulfilment_status = order_item_status as item_status_check
                            base_amount = total_delivered as revenue_check
                            from erp_dns_items e
                            left join karuru_dns_items k on e.name = k.code and e.item_code = k.product_bundle_id and e.uom = k.uom
                            --order by 1,2,5,6
                            ),
monthly_agg as (
                select distinct company,
                date_trunc(creation, month) as erp_creation_month,
                --date_trunc(posting_date, month) as posting_month,
                count(distinct name) as erp_dns_count,
                count(distinct code) as karuru_dns_count,
                count(distinct code) - count(distinct name) as dns_variance,
                sum(base_amount) as erp_revenue,
                sum(total_delivered) as karuru_revenue,
                sum(total_delivered) - sum(base_amount) as revenue_var
                from erp_vs_karuru_items_mashup e
                group by 1,2
                order by 1,2
                )

select *
from monthly_agg
--where check_uom is not true