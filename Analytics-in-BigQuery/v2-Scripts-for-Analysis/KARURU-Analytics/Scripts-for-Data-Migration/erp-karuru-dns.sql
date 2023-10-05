---------------- DNs  ------------------
--------- ERP vs. Karuru -----------
with
karuru_dns_with_index as (
                          SELECT *,
                          row_number()over(partition by id order by updated_at desc) as index
                          FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                          where date(created_at) between '2022-02-01' and '2023-09-23'
                          --where date(created_at) between '2023-09-03' and '2023-09-06'
                          --where date(created_at) between '2022-02-01' and '2022-12-31'
                          and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                          ),
karuru_dns as (
                select distinct created_at,
                id,
                code, 
                status,
                amount,
                is_pre_karuru,
                from karuru_dns_with_index where index = 1 
                and is_pre_karuru = true
                --and status in ('PAID')
                --and code = 'DN-RUAI-RXTS'
                ),
erp_dns_with_index as (
                        SELECT *, 
                        row_number()over(partition by name order by modified desc) as index 
                        FROM `kyosk-prod.erp_reports.delivery_note` 
                        where date(creation) between '2022-02-01' and '2023-08-31'
                        --where date(creation) between '2023-09-03' and '2023-09-06'
                        --where date(creation) between '2022-02-01' and '2022-12-31'
                        --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                        and company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
                        and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                        ),
erp_dns as (
            select distinct date(creation) as creation,
            name, 
            case
              when workflow_state in ('CANCELLED') then 'DRIVER_CANCELLED'
              when workflow_state in ('SUBMITTED', 'PROCESSING') then 'CREATED'
              when workflow_state in ('DISPATCHING') then 'DELIVERING'
            else workflow_state end as workflow_state,
            grand_total
            from erp_dns_with_index
            where index = 1
            and workflow_state in ('PAID')
            ),
erp_agg as (
            select e.*,
            k.status,
            amount,
            status =  workflow_state as check
            from erp_dns e
            left join karuru_dns k on e.name = k.code
            ),
monthly_agg as (
                  select date_trunc(e.creation, month) as creation_month,
                  count(distinct e.name) as erp_dns,
                  count(distinct k.code) as karuru_dns,
                  sum(e.grand_total) as erp_grand_total,
                  SUM(k.amount) as karuru_amount,
                  SUM(k.amount) - sum(e.grand_total) as amount_variance
                  from erp_dns e
                  left join karuru_dns k on e.name = k.code
                  group by 1
                  order by 1 desc
                  )
select *
from monthly_agg

--where check is false
--order by 1 desc, 2
--select * from erp_dns where name in (select distinct code from karuru_dns)