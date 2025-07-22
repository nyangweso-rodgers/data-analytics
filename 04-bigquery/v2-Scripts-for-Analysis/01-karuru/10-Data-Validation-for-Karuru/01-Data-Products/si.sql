------------------------- KARURU -----------------
-------------------- Sales Invoice ------------------------------------
with

karuru_si as (
              SELECT *,
              row_number()over(partition by id order by modified desc) as index
              FROM `kyosk-prod.karuru_reports.sales_invoice`
              where date(created) = '2024-04-01'
              --WHERE date(created) >= '2022-01-01'
              --and docstatus = 1
              --and is_karuru_applied = true
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
              ),
si_summary as (
                select distinct date(created) as created,
                si.company_id,
                si.name,
                --si.po_no,
                si.kyosk_delivery_note,
                --sii.sales_order,
                grand_total,
                from karuru_si si
                where index =1
                --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                --and company_id = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                --and company_id = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                )

select distinct company_id
from si_summary