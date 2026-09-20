with
discount_notes_cte as (
	SELECT id, accountId, discount_amount, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.discount_notes
	)
select #*
count(*)
from discount_notes_cte
limit 1000