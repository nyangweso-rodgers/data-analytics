WITH 
products_cte as (
	SELECT id, product, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.products
	)
select count(*)
from products_cte