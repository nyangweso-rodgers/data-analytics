WITH
--------------------- users ----------------------------------
users_cte AS (
    SELECT *
    FROM (
        SELECT *,
        row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rnk
        FROM zammad_production.users
        ) WHERE rnk = 1
    )
select *
from users_cte
LIMIT 100