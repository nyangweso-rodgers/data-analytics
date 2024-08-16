
----------- Service Providers ----------
with
service_provider as (
                      SELECT *,
                      row_number()over(partition by id order by updated_at desc) as index
                      FROM `kyosk-prod.karuru_reports.service_provider` 
                      WHERE date(created_at) > "2021-01-01"
                      
                      ),

service_provider_cte as (
                          select distinct created_at,
                          updated_at,
                          bq_upload_time,
                          country_code,
                          company_code,
                          id,
                          name,
                          owner,
                          provider_type,
                          disabled,
                          is_transporter,
                          supplier_group,
                          from service_provider
                          where index = 1
                          )
select *
from service_provider_cte
where name not in ("Test TZ Supplier")
--where name in ('WASHA DATA COMPANY LIMITED')
--and id in ('0GGWZH9MRQERG', '0GAGGD5N6Y9AN')
and id in ('0GGWZH9MRQERG')