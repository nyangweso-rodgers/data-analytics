with
users_cte as (
	SELECT id, organization_id, login, firstname, lastname, email, image, image_source, web, "password", phone, fax, mobile, 
	street, zip, city, country, vip, verified, active, note, last_login, "source", login_failed, out_of_office, 
	out_of_office_start_at, out_of_office_end_at, out_of_office_replacement_id, preferences, updated_by_id, created_by_id, created_at, updated_at, customerid, leadid, status, nationalid, deleted_at
	FROM public.users
	)
select *
--count(*)
--max(updated_at)
from users_cte
limit 1000