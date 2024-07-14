with
karuru_credit_disbursements as (
                                SELECT *, 
                                row_number()over(partition by id order by bq_upload_time desc) as index
                                FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                                WHERE date(disbursement_date) > '2023-08-01'
                                --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 4 month)
                                )
select distinct credit_type
from karuru_credit_disbursements
where index = 1