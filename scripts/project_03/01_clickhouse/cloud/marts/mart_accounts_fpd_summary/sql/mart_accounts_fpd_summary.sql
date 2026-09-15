WITH
mart_accounts_fpd_summary_cte as (
    SELECT * 
    FROM marts.mart_accounts_fpd_summary
    where first_expected_date = '2026-09-10'
    and is_fpd_amount = 1
    )
select count(*)
from mart_accounts_fpd_summary_cte
limit 1000