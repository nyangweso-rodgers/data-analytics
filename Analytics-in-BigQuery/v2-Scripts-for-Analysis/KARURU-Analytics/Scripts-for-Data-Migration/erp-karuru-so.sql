------------ ERPNext vs. Karuru 
--------------- SO ---------------------
with
erp_so as (
            SELECT *,
            row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            where date(creation) between '2022-02-01' and '2023-11-01'
            and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_order_items as (
                    select distinct so.name,
                    so.workflow_state,
                    date(creation) as creation,
                    --sales_partner
                    from erp_so so
                    where index = 1
                    ),
karuru_so as (
              SELECT *,
              row_number()over(partition by name  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              WHERE date(created_date) between '2022-02-01' and '2023-11-01'
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
              and is_pre_karuru = true
              ),
karuru_order_items as (
                        select distinct so.name,
                        date(created_date) as created_date,
                        so.order_status,
                        --market_developer_name
                        from karuru_so so
                        where index = 1
                        ),
mashup as (
            select e.*,
            k.created_date,
            --k.market_developer_name,
            e.creation = k.created_date as check_date,
            --e.sales_partner = k.market_developer_name as md_check
            k.order_status
            from erp_order_items e
            left join karuru_order_items k on e.name = k.name
            )
select *
from mashup
where check_date = false
order by 1,2