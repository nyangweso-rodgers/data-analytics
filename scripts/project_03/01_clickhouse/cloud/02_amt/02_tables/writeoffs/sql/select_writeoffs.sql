WITH
--------------------- Write Offs ----------------------------------
writeoffs_cte as (
    select *
    from (
        SELECT id,
        accountId,
        amount,
        coalesce(woDate, createdAt) as woDate,
        writeOffReason,
        createdAt,
        updatedAt,
        sync_at,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.writeoffs
     ) where rnk = 1
    )
select *
--count(*)
--distinct id, count(*) as record_count
--distinct writeOffReason, count(distinct accountId)
from writeoffs_cte
--GROUP BY 1 having record_count > 1 -- ORDER BY 2 desc
--where woDate is null
limit 100