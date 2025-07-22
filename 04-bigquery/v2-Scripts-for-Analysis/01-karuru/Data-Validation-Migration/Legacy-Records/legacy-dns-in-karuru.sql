--------------------- Karuru ---------------
------------------ Legacy DNs  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` 
                where date(created_at) between '2022-02-01' and '2023-11-08'
                --and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --and date(created_at) > '2023-08-05'
                and is_pre_karuru = true
                ),
legacy_dns as (
              select distinct date(created_at) as created_at,
              country_code,
              id,
              code,
              from karuru_dns dn
              where index = 1
              --and country_code = 'TZ'
              --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              --and dni.status = 'ITEM_FULFILLED'
              )
select count(distinct id), count(distinct code)
from legacy_dns
where code = 'DN-LZRA-QMKP'
--order by 1 desc, code, product_bundle_id