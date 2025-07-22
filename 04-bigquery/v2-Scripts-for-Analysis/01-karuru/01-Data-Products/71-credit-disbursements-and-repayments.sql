------------------- credit disbursements and repayments -----------------
with
credit_disbursements as (
                          SELECT *, 
                          row_number()over(partition by id order by bq_upload_time desc) as index
                          FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                          WHERE date(disbursement_date) > '2023-08-01'
                          --where date_trunc(date(disbursement_date), month) = '2024-06-01'
                          --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 7 month)
                          --and id = '0FG75T6HF3SPB'
                          --and payment_request_id= '0HHRZ816ME1EK'
                          --and customer_id in ('86600', '526094', '100259')
                          --and id = '0FZA0CX5YKYDP'
                          ), 
credit_disbursements_cte as (
                            select distinct cd.bq_upload_time,
                            cd.disbursement_date,
                            cd.due_date,
                            cd.customer_id,
                            cd.id,
                            cd.credit_status,
                            --cd.credit_status_description,
                            cd.payment_request_id,
                            cd.credit_amount,
                            cd.service_fee_amount,

                            cd.financier.name as credit_financier_name,

                            cd.credit_terms.service_fee_percentage,
                            cd.credit_terms.late_fee_per_day,
                            cd.credit_terms.credit_period,
                            cd.credit_terms.max_penalty_days,
                            from credit_disbursements cd--, unnest(credit_terms) ct
                            where index = 1
                            ),
credit_repayments_cte as (
                            select distinct 
                            cd.id as credit_id,
                            row_number()over(partition by cd.id order by r.audit.created asc) as credit_repayment_index,
                            r.audit.created as credit_repayment_creation_datetime,
                            r.id as credit_repayment_id,
                            r.payment_request_id as credit_repayments_payment_request_id,
                            r.principal_allocated,
                            r.service_fee_allocated,
                            r.amount,
                            r.is_complete,
                            r.reference,
                            --r.transaction_reference
                            from credit_disbursements cd, unnest(repayments) r
                            where index = 1
                            ),
------------------------- dns ---------------------
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 6 month)
                --where date_trunc(date(created_at), month) = '2024-06-01'
                where date(created_at) between '2023-08-01' and '2024-08-31'
                ),
dns_cte as (
            select distinct --created_at,
            country_code,
            territory_id,

            --delivery_trip_id,
            --dn.outlet_id,
            outlet.name as outlet_name,
            --outlet.phone_number as outlet_phone_number,
            --id,
            --row_number()over(partition by dn.delivery_trip_id order by dn.created_at) as delivery_note_created_at_index,
            dn.code,
            --dn.status,
            --dn.payment_type,
            dn.payment_request_id,
            --dn.amount
            from delivery_notes dn
            ),
/*
dns_settlement_cte as (
                        select distinct dn.id,
                        s.transaction_time,
                        row_number()over(partition by dn.id order by s.transaction_time asc) as dn_settlement_index,
                        s.channel as settlement_channel,
                        s.transaction_reference,
                        s.amount as settlement_amount,
                        from delivery_notes dn, unnest(settlements) s
                        where index = 1
                        ),*/
--------------------------- payment requests -------------------
payment_requests as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index
                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                --WHERE DATE(created_at) >= "2023-01-01" 
                where date(created_at) between '2023-08-01' and '2024-08-31'
                --where DATE(created_at) <= '2024-01-07'
                --where DATE(created_at) between '2024-01-01' and '2024-10-30' 
                --where date(created_at) >= date_sub(date_trunc(current_date, month),interval 1 week)
                --and country_code = 'KE'
                --and payment_reference = '0HF8VVFEV7G6A'
                --and payment_reference = '0GATQ0A048TZY'
                --and purpose = 'CREDIT_REPAYMENT'
                --and id = '0GD58XQQHGK65'
                --and payment_reference = '0GAJT0FRJ771W'
                --and payment_reference = '0GAJT0FRJ771W'
                ),
payment_request_settlements_cte as (
                        select distinct pr.created_at,
                        pr.id, 
                        s.transaction_time,
                        row_number()over(partition by pr.id order by s.transaction_time asc) as payment_request_settlement_index,
                        s.status,
                        s.transaction_reference,
                        s.channel,
                        s.settlement_type,
                        s.amount,
                        from payment_requests pr, unnest(settlement) s
                        where index = 1
                        ),
-------------------- Mashup -------------------
/*
credit_disbursements_and_repayments as (
                                        select distinct 

                                        dn.country_code,
                                        dn.territory_id,

                                        cd.customer_id,
                                        --dn.outlet_id,
                                        dn.outlet_name,

                                        date(dn.created_at) as dn_creation_date,
                                        dn.id as dn_id,
                                        dn.code as dn_code,
                                        dn.status as dn_status,
                                        dn.amount as dn_amount,
                                        dn.payment_type as dn_payment_type,
                                        --cd.payment_request_id,

                                        dnss.transaction_time as dn_settlement_transaction_time,
                                        dnss.dn_settlement_index,
                                        dnss.settlement_channel,
                                        dnss.transaction_reference as dn_settlement_transaction_reference,
                                        dnss.settlement_amount as dn_settlement_amount,

                                        cd.disbursement_date as credit_disbursement_date,
                                        cd.due_date as credit_due_date,
                                        cd.id as credit_id,
                                        cd.credit_status,
                                        cd.credit_status_description,


                                        --cr.credit_repayment_id,
                                        --cr.is_complete  as credit_repayment_is_complete,
                                        --cr.amount as credit_repayment_amount,
                                        --cr.principal_allocated as credit_repayment_principal_allocated,
                                        --cr.reference as credit_repayment_reference,

                                        --prs.created_at as payment_request_creation_datetime,
                                        --prs.id as payment_request_id,
                                        --prs.transaction_reference as payment_request_transaction_reference
                                        from credit_disbursements_cte cd
                                        left join dns_cte dn on cd.payment_request_id = dn.payment_request_id
                                        left join dns_settlement_cte dnss on dn.id = dnss.id
                                        --left join credit_repayments_cte cr on cd.id = cr.credit_id
                                        --left join payment_request_settlements_cte prs on cr.credit_repayment_id = prs.id
                                        ),*/
credit_repayment_tracker_cte as (
                                select distinct cd.customer_id, 
                                cd.disbursement_date as credit_disbursement_date,
                                cd.due_date as credit_due_date,
                                cd.id as credit_id,
                                cd.credit_financier_name,
                                cd.credit_status,
                                --cd.credit_status_description,
                                cd.credit_amount,
                                cd.service_fee_percentage,
                                cd.service_fee_amount,
                                cd.late_fee_per_day,
                                cd.credit_period,
                                cd.max_penalty_days,

                                cr.credit_repayment_creation_datetime,
                                cr.credit_repayment_index,
                                cr.credit_repayment_id,
                                cr.is_complete as credit_repayment_is_complete,

                                cr.amount as credit_repayment_amount,
                                cr.service_fee_allocated as credit_repayment_service_fee_allocated,
                                cr.principal_allocated as credit_repayment_principal_allocated,
                                cr.reference as credit_repayment_reference,
                                --cr.transaction_reference as credit_repayment_transaction_reference,
                                cr.credit_repayments_payment_request_id,

                                
                                prs.transaction_time as payment_request_settlement_transaction_time,
                                prs.payment_request_settlement_index,
                                prs.status as payment_request_settlement_status,
                                prs.channel as payment_request_settlement_channel,
                                prs.settlement_type as payment_request_settlement_settlement_type,
                                prs.amount as payment_request_settlement_amount,
                                prs.transaction_reference as payment_request_transaction_reference,

                                dn.country_code,
                                dn.territory_id,
                                dn.code as dn_code,
                                dn.outlet_name
                                from credit_disbursements_cte cd
                                left join credit_repayments_cte cr on cd.id = cr.credit_id
                                left join payment_request_settlements_cte prs on cr.credit_repayments_payment_request_id = prs.id
                                left join dns_cte dn on cd.payment_request_id = dn.payment_request_id
                                order by customer_id, credit_id, credit_repayment_index
                                )
select distinct credit_repayments_payment_request_id from credit_repayment_tracker_cte where (credit_repayments_payment_request_id is not null) and (payment_request_transaction_reference is null)
and territory_id not in ('Test KE Territory', 'Test UG Territory', 'Test TZ Territory')
--and FORMAT_DATE('%Y%m%d', date(credit_disbursement_date)) between @DS_START_DATE and @DS_END_DATE
--order by 1,2
