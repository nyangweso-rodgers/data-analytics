----------------------------------- karuru - Payment Entry --------------------------
with
payment_entry as (
                  SELECT *,
                  row_number()over(partition by id  order by modified desc) as index
                  FROM `kyosk-prod.karuru_reports.payment_entry` 
                  --WHERE date(creation) < "2024-01-01"
                  WHERE date(creation) between "2022-03-01" and '2023-12-31'
                  and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                  --and payment_type = 'Receive'
                  --and name = 'ACC-PAY-2022-1642575'
                  and name = "ACC-PAY-2022-1642575"
                  ),
payment_entry_cte as (
                      select distinct pe.creation,
                      pe.modified,
                      pe.posting_date,
                      pe.bq_upload_time,

                      pe.company_id,
                      --pe.territory_code,
                      pe.territory,

                      pe.payment_type,
                      pe.mode_of_payment,
                      pe.payment_method_sub_category,


                      pe.party_type,
                      pe.party,
                      pe.dn_id, 
                      pe.payment_reference,
                      pe.reference_no,

                      r.reference_name,
                      r.total_amount,
                      r.outstanding_amount,
                      r.allocated_amount,

                      --pe.paid_from,
                      pe.paid_from_account_currency,
                      pe.paid_to_account_type,
                      pe.paid_amount,
                      from payment_entry pe, unnest(references) r
                      where index =1
                      --and payment_type = 'Pay'
                      --and posting_date >= '2024-01-01'
                      )
                      
select *
--distinct payment_type
--distinct party, sum(paid_amount) as paid_amount
--min(creation) as min_creation_datetime, min(modified) as min_modified_datetime
from payment_entry_cte