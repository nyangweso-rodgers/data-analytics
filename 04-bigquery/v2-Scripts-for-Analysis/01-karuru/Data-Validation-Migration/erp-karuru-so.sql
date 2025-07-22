------------ ERPNext vs. Karuru 
--------------- SO ---------------------
with
erp_so as (
            SELECT *,
            row_number()over(partition by name order by modified desc) as index
            FROM `kyosk-prod.erp_reports.sales_order` 
            where date(creation) between '2022-02-01' and '2023-12-10'
            and territory not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
            ),
erp_so_report as (
                  select distinct so.name,
                  case
                    when so.workflow_state = 'PAID' then 'COMPLETED'
                    --when so.workflow_state = 'DELIVERED' then 'DELIVERED'
                    --when so.workflow_state = 'USER_CANCELLED' then 'USER_CANCELLED'
                    --6when so.workflow_state = 'DISPATCHED' then 'DISPATCHED'
                    --when so.workflow_state = 'SUBMITTED' then 'SUBMITTED'
                    --when so.workflow_state = 'COMPLETED' then 'COMPLETED'
                    --when so.workflow_state = 'INITIATED' then 'INITIATED'
                    --when so.workflow_state = 'EXPIRED' then 'EXPIRED'
                    when so.workflow_state = 'DRIVER_CANCELLED' then 'CANCELLED'
                    when so.workflow_state = 'EXPIRED_PROCESSING' then 'EXPIRED'
                    --when so.workflow_state = 'PROCESSING' then 'PROCESSING'
                    --when so.workflow_state = 'DISPATCHING' then 'DISPATCHING'
                    --when so.workflow_state = 'SOLD_ON_CREDIT' then 'SOLD_ON_CREDIT' 
                    when so.workflow_state = 'OMS_CANCELLED' then 'CANCELLED'
                    --when so.workflow_state = 'DELIVERING' then 'DELIVERING'
                    --when so.workflow_state = 'CANCELLED' then 'CANCELLED'
                    when so.workflow_state = 'DRIVER_RESCHEDULED' then 'RESCHEDULED'
                  else so.workflow_state end as workflow_state,
                  date(creation) as creation,
                  sales_partner
                  from erp_so so
                  where index = 1
                  --and name in ('SAL-ORD-EMBCXQL')
                  ),
karuru_so as (
              SELECT *,
              row_number()over(partition by name  order by last_modified_date desc) as index
              FROM `kyosk-prod.karuru_reports.sales_order` so
              WHERE date(created_date) between '2022-02-01' and '2023-12-31'
              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
              and is_pre_karuru = true
              ),
karuru_so_report as (
                      select distinct so.name,
                      date(created_date) as created_date,
                      so.order_status,
                      market_developer_name,
                      so.bq_upload_time
                      from karuru_so so
                      where index = 1
                      ),
mashup as (
            select e.*,
            k.created_date,
            k.order_status,
            k.market_developer_name,
            e.creation = k.created_date as check_date,
            e.sales_partner = k.market_developer_name as check_md,
            e.workflow_state = k.order_status as check_status,
            k.bq_upload_time
            from erp_so_report e
            left join karuru_so_report k on e.name = k.name
            )
select *
from mashup
where (check_date = false) OR (check_status = false)-- or (check_md = false)
order by creation desc