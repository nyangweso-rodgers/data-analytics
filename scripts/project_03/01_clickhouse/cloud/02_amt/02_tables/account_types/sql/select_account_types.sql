WITH
--------------------- Account Types ----------------------------------
account_types_cte as (
        select * 
        from (
                SELECT id,
                accountType,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM amt.account_types
                ) where rnk = 1
        )
select *
from account_types_cte