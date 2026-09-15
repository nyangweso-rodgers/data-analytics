WITH
mart_all_tickets_cte as (
    SELECT * 
    FROM mart_all_tickets 
    )
select *
--max(_generated_at), min(created_date), max(created_date)
from mart_all_tickets_cte
LIMIT 1000