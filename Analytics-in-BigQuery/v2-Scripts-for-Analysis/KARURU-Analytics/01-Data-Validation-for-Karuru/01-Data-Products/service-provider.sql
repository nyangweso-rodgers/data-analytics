------------ Karuru ------------
----------- Service Providers ----------
with
karuru_service_provider as (
                            SELECT *,
                            row_number()over(partition by id order by updated_at desc) as index
                            FROM `kyosk-prod.karuru_reports.service_provider` 
                            WHERE date(created_at) > "2021-01-01"
                            and name not in ("Test TZ Supplier")
                            )
select *
from karuru_service_provider
--where index = 1
where name in ('WASHA DATA COMPANY LIMITED')
order by name