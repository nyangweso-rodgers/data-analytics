------------------- credit disbursements -----------------
with
credit_disbursements as (
                          SELECT *, 
                          row_number()over(partition by id order by bq_upload_time desc) as index
                          FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                          --WHERE date(disbursement_date) > '2023-08-01'
                          where date_trunc(date(disbursement_date), month) = '2024-06-01'
                          --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 7 month)
                          --and id = '0FG75T6HF3SPB'
                          --and payment_request_id= '0HHRZ816ME1EK'
                          --and customer_id = '100259'
                          --and customer_id = '526094'
                          and customer_id = '86600'
                          ), 
credit_disbursements_cte as (
                            select distinct cd.bq_upload_time,
                            cd.disbursement_date,
                            cd.due_date,
                            cd.customer_id,
                            cd.id,
                            cd.credit_status,
                            cd.credit_status_description,
                            cd.payment_request_id
                            from credit_disbursements cd
                            where index = 1
                            ),
credit_repayments_cte as (
                            select distinct 
                            cd.id as credit_id,
                            r.id as credit_repayment_id,
                            r.payment_request_id as repayments_payment_request_id,
                            --r.credit_id as repayments_credit_id,
                            r.principal_allocated,
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
                where date_trunc(date(created_at), month) = '2024-06-01'
                ),
delivery_notes_cte as (
                        select distinct created_at,
                        country_code,
                        territory_id,

                        --delivery_trip_id,
                        --dn.outlet_id,
                        outlet.name as outlet_name,
                        --outlet.phone_number as outlet_phone_number,
                        id,
                        --row_number()over(partition by dn.delivery_trip_id order by dn.created_at) as delivery_note_created_at_index,
                        dn.code,
                        dn.status,
                        dn.payment_request_id,
                        from delivery_notes dn
                        ),
dns_settlement_cte as (
                        select distinct dn.id,
                        s.channel as settlement_channel,
                        s.transaction_reference,
                        s.amount as settlement_amount,
                        from delivery_notes dn, unnest(settlements) s
                        where index = 1
                        --and s.status not in ('INITIATED')
                        ),
--------------------------- payment requests -------------------
payment_requests as (
                SELECT *,
                row_number()over(partition by id order by last_modified desc) as index
                FROM `kyosk-prod.karuru_reports.payment_requests` pr
                WHERE DATE(created_at) >= "2023-01-01" 
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
pr_settlements_cte as (
                        select distinct pr.created_at,
                        pr.id, 
                        s.status as settlement_status,
                        s.transaction_reference,
                        s.channel,
                        s.settlement_type,
                        s.amount as settlement_amount
                        from payment_requests pr, unnest(settlement) s
                        where index = 1
                        ),
-------------------- Mashup -------------------
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
                                        --cd.payment_request_id,
                                        dnss.settlement_channel as dn_settlement_channel,
                                        dnss.transaction_reference as dn_transaction_reference,
                                        dnss.settlement_amount as dn_settlement_amount,

                                        cd.disbursement_date as credit_disbursement_date,
                                        cd.due_date as credit_due_date,
                                        cd.id as credit_id,
                                        cd.credit_status,
                                        cd.credit_status_description,


                                        cr.credit_repayment_id,
                                        cr.is_complete  as credit_repayment_is_complete,
                                        cr.amount as credit_repayment_amount,
                                        cr.principal_allocated as credit_repayment_principal_allocated,
                                        cr.reference as credit_repayment_reference,

                                        prs.created_at as payment_request_creation_datetime,
                                        prs.id as payment_request_id,
                                        prs.transaction_reference as payment_request_transaction_reference
                                        from credit_disbursements_cte cd
                                        left join delivery_notes_cte dn on cd.payment_request_id = dn.payment_request_id
                                        left join dns_settlement_cte dnss on dn.id = dnss.id
                                        left join credit_repayments_cte cr on cd.id = cr.credit_id
                                        left join pr_settlements_cte prs on cr.credit_repayment_id = prs.id
                                        )
select * from credit_disbursements_and_repayments