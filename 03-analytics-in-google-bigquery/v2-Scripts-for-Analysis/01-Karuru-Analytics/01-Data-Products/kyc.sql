
------------------ KYC --------
with
kyc as (
        SELECT *,
        row_number()over(partition by retailer_id order by updated_at desc ) as index
        FROM `kyosk-prod.karuru_reports.kyc` 
        WHERE date(created_at) > "2022-02-07"
        and kyosk_territory not in ('Kiambu Test','Kyosk HQ', 'global - product team', 'Test KE Territory', 'Test KE Territory', 'Test UG Territory', 'Test NG Territory', 'Test TZ Territory', 'DKasarani')
        ),
kyc_cte as (
            select distinct created_at as created_at,
            updated_at as updated_at, 
            bq_upload_time,
            country,
            kyosk_territory, 
            retailer_id, 
            owner_phone_number,
            kyc_status, 
            --narration
            service_name, 
            from kyc
            where index = 1
            )
select distinct retailer_id, count(distinct service_name) service_name
from kyc_cte
--where retailer_id = '0CW8ZBKGMZWT8'
group by 1
having service_name > 1