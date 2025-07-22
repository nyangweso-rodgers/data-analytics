----------------- Karuru ----------------
-------------- DNs vs. SI -----------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                and date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dns_summary as (
              select distinct --date(created_at) as created_at,
              date(delivery_date) as delivery_date,
              country_code,
              id,
              code,
              dn.status,
              from karuru_dns dn
              where index = 1
              AND dn.status IN ('PAID', 'DELIVERED')
              ),
karuru_si as (
              SELECT *,
              row_number()over(partition by id order by modified desc) as index
              FROM `kyosk-prod.karuru_reports.sales_invoice`
              WHERE date(created) >= '2023-08-05'
              and is_karuru_applied = true
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
              ),
si_summary as (
                select distinct date(created) as created,
                si.name,
                si.kyosk_delivery_note,
                from karuru_si si
                where index =1
                ),
dn_si_summary as (
                  select dn.*,
                  si.name,
                  si.created as si_creation_date,
                  date_diff(si.created, delivery_date, day) as date_var
                  from dns_summary dn
                  left join si_summary si on dn.id = si.kyosk_delivery_note
                  )
select * from dn_si_summary
where date_var <> 0
and FORMAT_DATE('%Y%m%d', delivery_date) between @DS_START_DATE and @DS_END_DATE  
order by date_var desc