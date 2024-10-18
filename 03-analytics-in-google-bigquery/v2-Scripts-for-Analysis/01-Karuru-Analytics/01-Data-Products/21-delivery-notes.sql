------------ Delivery Notes ----------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory', 'Test Fresh TZ Territory')
                --where territory_id in ('Voi')
                and date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 1 month)
                --and date(created_at) = '2024-04-16' 
                --and code = 'DN-NLKL-0HHPPMEK11X2N'
                --and id = '0HJQB8SV7DPQT'
                --and id = '0HJQB8SV7DPQT'
                --and status in ('PAID')

                ),
delivery_notes_cte as (
                        select distinct created_at,
                        --date(created_at) as dn_created_at,
                        dn.delivery_window_id,
                        dn.delivery_window.delivery_date  as delivery_window_delivery_date,
                        dn.scheduled_delivery_date,
                        
                        dn.delivery_date,

                        dn.country_code,
                        dn.territory_id,
                        dn.delivery_trip_id,
                        dn.id,
                        dn.code,
                        dn.status,
                        dn.payment_request_id,
                        dn.outlet_id,
                        outlet.name as outlet_name,
                        outlet.phone_number as outlet_phone_number,
                        from delivery_notes dn
                        where index = 1
                        --and s.status not in ('INITIATED')
                        )
select *
from delivery_notes_cte
where scheduled_delivery_date = '2024-10-19'
--where scheduled_delivery_date is null
--where country_code = 'KE'
order by delivery_date desc