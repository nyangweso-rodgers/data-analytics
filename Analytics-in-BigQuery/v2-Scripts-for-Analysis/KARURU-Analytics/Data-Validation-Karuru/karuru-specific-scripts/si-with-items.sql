------------------------- KARURU -----------------
-------------------- Sales Invoice Item ------------------------------------
with

karuru_si as (
              SELECT *,
              row_number()over(partition by id order by modified desc) as index
              FROM `kyosk-prod.karuru_reports.sales_invoice`
              --where date(created) between '2023-09-01' and  '2023-09-10'
              --WHERE date(created) >= '2022-01-01'
              where date(created) between '2023-11-19'  and "2023-11-25" 
              --and docstatus = 1
              and is_karuru_applied = true
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
              ),
si_summary as (
                select distinct date(created) as created,
                si.name,
                si.po_no,
                --si.kyosk_delivery_note,
                --sii.sales_order,
                --sii.delivery_note,
                --grand_total,
                sii.item_code,
                sii.uom,
                sii.conversion_factor,
                sii.qty,
                sii.stock_qty
                from karuru_si si, unnest(items) sii
                where index =1
                --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                --and company_id = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                --and company_id = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' 
                and si.territory_id in ('Kawempe', 'Luzira', 'Mukono')
                and item_code = 'Velvex Air Freshener Lavender And Chamomile 300ML PC (1 Pc)'
                )

select *
from si_summary