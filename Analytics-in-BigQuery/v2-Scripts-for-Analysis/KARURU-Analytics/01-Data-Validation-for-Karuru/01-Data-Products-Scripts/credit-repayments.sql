
with
karuru_credit_disbursements as (
                                SELECT *, 
                                row_number()over(partition by id order by bq_upload_time desc) as index
                                FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                                WHERE date(disbursement_date) > '2023-08-01'
                                --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 4 month)
                                ),
karuru_credit_repayments as (
                            select distinct
                            r.credit_id,
                            r.id,
                            r.principal_allocated,
                            r.service_fee_allocated,
                            --r.penalties_allocated,
                            r.amount,
                            r.audit.created,
                            r.audit.last_modified,
                            last_value(r.audit.last_modified)over(partition by r.credit_id order by r.audit.last_modified asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as repayment_date
                            from karuru_credit_disbursements c, unnest(repayments) r
                            where index = 1
                            )
select *
from karuru_credit_repayments
where credit_id = '0FA0CQQ8RAPA8'
order by created