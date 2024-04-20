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
dns_items as (
              select distinct date(delivery_date) as delivery_date,
              --country_code,
              --territory_id,
              id,
              code as dn_code,
              dn.status as dn_status,
              oi.status as dn_item_status,
              oi.product_bundle_id,
              oi.uom,
              sum(oi.total_delivered) as total_delivered
              from karuru_dns dn, unnest(order_items) oi
              where index = 1
              --AND dn.status IN ('PAID', 'DELIVERED', 'CASH_COLLECTED')
              --and oi.status = 'ITEM_FULFILLED'
              group by 1,2,3,4,5,6,7
              ),
karuru_si as (
              SELECT *,
              row_number()over(partition by id order by modified desc) as index
              FROM `kyosk-prod.karuru_reports.sales_invoice`
              --WHERE date(created) >= '2023-08-05'
              WHERE date(created) between '2024-01-01' and '2024-03-31'
              --and is_karuru_applied = true
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
              ),
si_items as (
              select distinct date(created) as created,
              si.company_id,
              si.territory_id,
              si.name,
              si.kyosk_delivery_note,
              sii.item_code,
              sii.uom
              from karuru_si si,unnest(items) sii
              where index =1
              ),
si_and_dns_items as (
                      select sii.*,
                      dni.*except(id,uom)
                      from si_items sii
                      left join dns_items dni on sii.kyosk_delivery_note = dni.id and sii.item_code = dni.product_bundle_id and sii.uom = dni.uom 
                      )
select *  from si_and_dns_items
--where  FORMAT_DATE('%Y%m%d', delivery_date) between @DS_START_DATE and @DS_END_DATE  
--and date_var <> 0
--where company_id in ('KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED')
--and name in ('SI-PHOB-0FG2X1HT6HVHC-2024')
--where dn_item_status not in ('ITEM_FULFILLED')
where dn_code = 'DN-JCNC-0F99YREHEHKWA'
order by company_id, territory_id, name