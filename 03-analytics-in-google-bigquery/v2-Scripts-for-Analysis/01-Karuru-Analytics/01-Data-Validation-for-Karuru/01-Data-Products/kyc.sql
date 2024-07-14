
------------------ KYC Data --------
with
karuru_kyc as (
                SELECT *,
                row_number()over(partition by retailer_id order by updated_at desc ) as index
                FROM `kyosk-prod.karuru_reports.kyc` 
                WHERE date(created_at) > "2022-02-07"
                and kyosk_territory not in ('Kiambu Test','Kyosk HQ', 'global - product team', 'Test KE Territory', 'Test KE Territory', 'Test UG Territory', 'Test NG Territory', 'Test TZ Territory', 'DKasarani')
                ),
kyc_report as (
                select distinct created_at as created_at,
                updated_at as updated_at, 
                retailer_id, 
                kyosk_territory, 
                country, 
                kyc_status, 
                --narration
                service_name,
                bq_upload_time
                from karuru_kyc
                where index = 1
                )
select * from kyc_report
where retailer_id = '0CWKJKX5J010K'