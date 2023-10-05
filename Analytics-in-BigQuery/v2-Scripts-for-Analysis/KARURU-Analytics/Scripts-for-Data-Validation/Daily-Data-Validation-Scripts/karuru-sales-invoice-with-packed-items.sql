------------------------- KARURU - 
----------Sales Invoice with packed items ------------------------------------
with

sales_invoice_with_index as (
                              SELECT *,
                              row_number()over(partition by id order by modified desc) as index
                              FROM `kyosk-prod.karuru_reports.sales_invoice`
                              where date(created) between '2023-08-01' and  '2023-09-06'
                              --WHERE date(created) >= date_sub(current_date, interval 1 month)
                              and docstatus = 1
                              and is_karuru_applied = true
                              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                              and name = 'SI-MWEN-B5A6-2023'
                              ),
sales_invoice_with_items as (
                              select distinct date(created) as created,
                              si.name,
                              sii.item_code,
                              sii.item_id,
                              sii.item_name,
                              sii.so_detail,
                              sii.dn_detail,
                              sii.sales_invoice_item
                              from sales_invoice_with_index si, unnest(items) sii
                              where index =1
                              ),
sales_invoice_with_packed_items as (
                          select 
                          si.created,
                          si.name,
                          pi.parent_item_id,
                          pi.parent_detail_docname,
                          pi.item_code,
                          pi.incoming_rate
                          from sales_invoice_with_index si, unnest(packed_items) pi
                          where index =1
                          )

select sii.*,
pi.parent_detail_docname,
pi.item_code
from sales_invoice_with_items sii
left join sales_invoice_with_packed_items pi on sii.name = pi.name and sii.item_code = pi.parent_item_id
--where item_code is null