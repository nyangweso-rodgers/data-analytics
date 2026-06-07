WITH
--------------------- Refunds ----------------------------------
refunds_cte as (
    select *
    from (
        SELECT distinct id,
        accountId,
        paymentId,
        refundAmount,
        createdAt,
        updatedAt,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
    FROM amt.refunds
    ) where rnk = 1
    ),
--------------------- Validate Duplicate Ledger Entries ----------------------------------
validate_duplicate_cte as (
    select distinct id,
    count(*) as record_count
    from refunds_cte
    GROUP BY 1
    HAVING record_count > 1
) 
select *
--count(*)
--distinct status
--from refunds_cte
from validate_duplicate_cte
limit 1000