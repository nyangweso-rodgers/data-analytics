/*
- Discount Notes per Account Id
*/
WITH
discount_notes_cte as (
    select *
    from (
        SELECT distinct accountId,
        discountId,
        amount,
        note,
        ledgerEntryID,
        createdAt,
    row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk  
    FROM amt.discount_notes
    ) where rnk = 1
    ) 
select *
from discount_notes_cte