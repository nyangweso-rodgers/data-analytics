------------------------- KARURU ---------
---- Sales Invoice & DNs Items ----------------
with

sales_invoice_with_index as (
                              SELECT *,
                              row_number()over(partition by id order by modified desc) as index
                              FROM `kyosk-prod.karuru_reports.sales_invoice`
                              --WHERE date(created) >= date_sub(current_date, interval 1 month)
                              WHERE date(created) = '2023-09-01'
                              and docstatus = 1
                              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                              --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                              --and company_id = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                              --and company_id = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                              ),
sales_invoice_summary as (
                          select distinct si.company_id,
                          si.territory_id,
                          si.name,
                          si.kyosk_delivery_note,
                          si.po_no,
                          si.status,
                          sii.item_id,
                          sii.item_code,
                          sii.uom,
                          sum(sii.base_amount) as base_amount
                          from sales_invoice_with_index si, unnest(items) sii
                          where index =1
                          and is_karuru_applied = true
                          and name in ('SI-MWEN-PE90-2023', 'SI-MWEN-W9KN-2023', 'SI-THEM-2K6W-2023')
                          group by 1,2,3,4,5,6,7,8,9
                          ),
delivery_note_with_index as (
                              SELECT *,
                              row_number()over(partition by code order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                              where date(created_at) >= '2023-08-01'
                              and is_pre_karuru = false 
                              ),
dns_summary as (
                select distinct id,
                code,
                dn.status,
                dni.catalog_item_id,
                dni.product_bundle_id,
                dni.uom,
                sum(total_delivered) as total_delivered
                from delivery_note_with_index dn, unnest(order_items) dni
                where index = 1
                group by 1,2,3,4,5,6
                ),
invoice_dns_summary as (
                        select sis.*,
                        dns.code,
                        dns.status as dn_status,
                        dns.total_delivered
                        from sales_invoice_summary sis
                        left join dns_summary dns on sis.kyosk_delivery_note = dns.id and sis.item_code = dns.product_bundle_id and sis.uom = dns.uom
                        )

select *
from invoice_dns_summary
order by 1,2,3