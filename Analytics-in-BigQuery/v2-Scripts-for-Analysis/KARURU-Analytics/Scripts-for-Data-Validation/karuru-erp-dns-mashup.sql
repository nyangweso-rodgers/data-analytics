---------------- Dns, Karuru - ERP Mashup ------------------
with
karuru_dns_with_index as (
                              SELECT *,
                              row_number()over(partition by code order by updated_at desc) as index
                              FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                              WHERE DATE(created_at) >= date_sub(current_date, interval 3 month)
                              and date(delivery_date) = '2023-08-30'
                              and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                              ),
erp_dns_with_index as (
                        SELECT *, 
                        row_number()over(partition by name order by modified desc) as index 
                        FROM `kyosk-prod.erp_reports.delivery_note` 
                        where date(creation) >= '2022-02-01'
                        --and date(delivered_time) = '2023-08-30'
                        ),
karuru_monthly_dns_report as (
                                select distinct date(created_at) as created_at,
                                date(delivery_date) as delivery_date,
                                code, 
                                status,
                                from karuru_dns_with_index where index = 1 
                                and is_pre_karuru = true
                                and status in ('PAID', 'DELIVERED')
                                ),
erp_dns_monthly_summary as (
                            select distinct date(creation) as creation,
                            date(delivered_time) as delivered_date,
                            name, 
                            workflow_state,
                            grand_total
                            from erp_dns_with_index dn where index = 1
                            ),
karuru_erp_mashup as (
                        select k.*,
                        e.*except(name),
                        case 
                          when (status = 'DRIVER_CANCELLED' and workflow_state = 'CANCELLED') or (status = workflow_state) then true
                        else false end as check
                        from karuru_monthly_dns_report k
                        left join erp_dns_monthly_summary e  on k.code = e.name
                        )
select *
from karuru_erp_mashup
--where check is false --and workflow_state = 'PAID'