with
credit_disbursements as (
                                SELECT *, 
                                row_number()over(partition by id order by bq_upload_time desc) as index
                                FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                                WHERE date(disbursement_date) > '2023-08-01'
                                --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 4 month)
                                and id = '0FG75T6HF3SPB'
                                ),
credit_disbursements_cte as (
                            select distinct cd.bq_upload_time,
                            cd.disbursement_date,
                            cd.due_date,
                            cd.customer_id,
                            cd.id,
                            cd.credit_status,
                            cd.credit_status_description,
                            from credit_disbursements cd
                            where index = 1
                            ),
credit_repayments_cte as (
                            select distinct date(cd.disbursement_date) as credit_disbursement_date,
                            cd.id as credit_id,
                            r.id as repayment_id,
                            r.payment_request_id as repayments_payment_request_id,
                            r.credit_id as repayments_credit_id,
                            r.principal_allocated as repayments_principal_allocated,
                            r.amount as repayments_amount,
                            r.is_complete as repaymens_is_complete,
                            r.reference as repayments_reference,
                            from credit_disbursements cd, unnest(repayments) r
                            where index = 1
                            )
select * from credit_repayments_cte