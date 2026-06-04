with
customers_cte as (
	SELECT id, companyRegionId, name, phoneNumber, identificationNumber, latitude, longitude, walletID, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.customers
	)
SELECT  count(*)
from customers_cte