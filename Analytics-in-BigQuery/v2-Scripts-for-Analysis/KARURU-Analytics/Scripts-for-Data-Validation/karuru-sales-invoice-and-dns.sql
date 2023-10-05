------------------------- KARURU ---------
---- Sales Invoice & DNs ----------------
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
                          select distinct company_id,
                          territory_id,
                          name,
                          kyosk_delivery_note,
                          po_no,
                          status,
                          base_total
                          from sales_invoice_with_index
                          where index =1
                          and is_karuru_applied = true
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
                status,
                amount
                from delivery_note_with_index
                where index = 1
                ),
invoice_dns_summary as (
                        select sis.*,
                        dns.code,
                        dns.status as dn_status,
                        dns.amount as dn_amount
                        from sales_invoice_summary sis
                        left join dns_summary dns on sis.kyosk_delivery_note = dns.id
                        )

select *
from invoice_dns_summary
order by 1,2,3