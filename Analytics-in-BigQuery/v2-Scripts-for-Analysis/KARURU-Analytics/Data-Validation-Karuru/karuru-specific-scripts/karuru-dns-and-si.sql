--------------------- Karuru ---------------
------------------ DNs, Sales Invoices Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by code order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --and date(created_at) > '2023-07-01'
                and date(created_at) <= '2023-10-30'
                and is_pre_karuru = false
                ),
dns_report as (
                select distinct date(created_at) as created_at,
                country_code,
                territory_id,
                id,
                code,
                dn.status,
                dni.product_bundle_id,
                dn.retailer_id,
                from karuru_dns dn, unnest(order_items) dni
                where index = 1
                ),
karuru_si as (
                SELECT *,
                row_number()over(partition by id order by modified desc) as index
                FROM `kyosk-prod.karuru_reports.sales_invoice`
                WHERE date(created) >= '2023-07-01'
                and docstatus = 1
                and is_karuru_applied = true
                ),
si_report as (
              select date(created) as created,
              name,
              kyosk_delivery_note
              from karuru_si
              where index = 1
              ),
dn_si_report as (
                select dn.*,
                si.name,
                si.created
                from dns_report dn 
                left join si_report si on dn.id = si.kyosk_delivery_note
                )
select *
from dn_si_report
where FORMAT_DATE('%Y%m%d', created_at) between @DS_START_DATE and @DS_END_DATE
and status in ('PAID', 'DELIVERED') and name is null
--and territory_id = 'Ruai'
order by id