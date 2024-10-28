----------------------------------- Payment Entry --------------------------
with
payment_entry as (
                  SELECT *,
                  row_number()over(partition by id  order by modified desc) as index
                  FROM `kyosk-prod.karuru_reports.payment_entry` 
                  WHERE date(creation) > "2021-06-26"
                  and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                  ),
payment_entry_cte as (
                      select distinct posting_date,
                      company_id,
                      payment_type,
                      mode_of_payment,
                      party_type,
                      party,
                      paid_from,
                      paid_from_account_currency,
                      paid_to_account_type,
                      paid_amount,
                      from payment_entry
                      where index =1
                      and payment_type = 'Pay'
                      and posting_date >= '2024-01-01'
                      )
                      
select distinct party, sum(paid_amount) as paid_amount
from payment_entry_cte
group by 1
order by 2 desc