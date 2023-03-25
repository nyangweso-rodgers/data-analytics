with

customer_with_index as (
                        SELECT *,
                        row_number()over(partition by name order by modified desc) as index 
                        FROM `kyosk-prod.erp_reports.customer` 
                        where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                        and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                        )
select distinct name, kyosk_identity_code from customer_with_index where index = 1