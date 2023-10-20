-------------- Kiva Analysis Script -------------
with
loan_themes_by_region as (
                          SELECT distinct Partner_ID,
                          sector
                          FROM `general-364419.kiva_dataset.loan_themes_by_region` 
                          --where Partner_ID is not null
                          --where Partner_ID = 225
                          ),
kiva_loans as (
                SELECT distinct date,
                id,
                country,
                kv.partner_id,
                date(funded_time) as funded_date,
                case
                  when date(funded_time) is not null then 'YES' else 'NO'
                end as funded_status,
                date(disbursed_time) as disbursed_date,
                case
                  when date(disbursed_time) is not null then "YES" else "NO"
                end as disbursement_status,
                repayment_interval,
                term_in_months,
                currency,
                loan_amount,
                funded_amount,
                ltbr.sector

                FROM `general-364419.kiva_dataset.kiva_loans`  kv
                left join loan_themes_by_region ltbr on kv.partner_id = ltbr.Partner_ID
                )

select *
from kiva_loans 
where FORMAT_DATE('%Y%m%d', date) between @DS_START_DATE and @DS_END_DATE