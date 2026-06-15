with
premises_cte as (
	SELECT --meta, 
	account_id, 
	id, 
	created_at, 
	updated_at, 
	created_by, 
	updated_by, 
	is_active, 
	--old_premise_id, 
	premise_name, 
	customer_id, 
	premise_type_id, 
	premise_number, 
	substate_id, 
	town, 
	courier_location_id,
	is_validated
	FROM public.premises
	)	
select * 
--distinct customer_id
from premises_cte
where customer_id = '46826'
--where account_id = '58821'
/*where customer_id in ('4816',
'42229',
'42316',
'45319',
'46062',
'46490',
'46586',
'46736',
'46826',
'46884',
'46963',
'47011',
'47201',
'47608',
'48116',
'48343',
'48749',
'48972',
'49676',
'49679',
'50267',
'50336',
'52158',
'53002',
'53003',
'53227',
'53973',
'57442',
'57665',
'72125')
*/