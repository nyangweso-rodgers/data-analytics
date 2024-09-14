------------ Delivery Notes with Settlement ----------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 5 month)
                --where date(created_at) = '2024-04-16' and id = '0FRHJTEAVJP5P' 
                where date(created_at) = '2024-08-29' and id = '0H3Z4Z6ZFNG39'

                ),
delivery_notes_settlement_cte as (
                                    select distinct created_at,
                                    --date(created_at) as dn_created_at,
                                    country_code,
                                    dn.delivery_trip_id,
                                    id,
                                    dn.code,
                                    row_number()over(partition by dn.delivery_trip_id order by dn.created_at) as delivery_note_created_at_index,
                                    dn.status,
                                    dn.outlet_id,
                                    outlet.name as outlet_name,
                                    outlet.phone_number as outlet_phone_number,
                                    dn.payment_request_id,
                                    s.channel as settlement_channel,
                                    transaction_reference,
                                    s.amount as settlement_amount,
                                    from delivery_notes dn, unnest(settlements) s
                                    where index = 1
                                    --and s.status not in ('INITIATED')
                                    ),
delivery_notes_settlement_agg as (
                                  select distinct delivery_trip_id,
                                  id,
                                  code,
                                  delivery_note_created_at_index,
                                  count(distinct transaction_reference) as transaction_reference_count,
                                  string_agg(distinct settlement_channel) as settlement_channel,
                                  sum(settlement_amount) as settlement_amount
                                  from delivery_notes_settlement_cte
                                  group by 1,2,3,4
                                  )
select *
from delivery_notes
--where country_code = 'KE'