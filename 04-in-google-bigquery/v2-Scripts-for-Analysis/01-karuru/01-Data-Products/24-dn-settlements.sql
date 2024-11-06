------------ Delivery Notes Settlement ----------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
                and date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 6 month)
                --where date(created_at) = '2024-04-16' and id = '0FRHJTEAVJP5P' 
                --where date(created_at) = '2024-08-29' and id = '0H3Z4Z6ZFNG39'
                --where date(created_at) = '2024-09-16' and delivery_trip_id = '0H9Q8HJ8VXMMZ'
                --and date(created_at) = '2024-04-01' and delivery_trip_id = '0FKNVXB5SG849'
                --and id = '0FK28M12SGA26'
                and id = '0GA3XTVDY74B4'
                ),
dns_cte as (
            select distinct created_at,
            country_code,
            territory_id
            
            delivery_trip_id,
            dn.outlet_id,
            outlet.name as outlet_name,
            outlet.phone_number as outlet_phone_number,
            id,
            row_number()over(partition by dn.delivery_trip_id order by dn.created_at) as delivery_note_created_at_index,
            dn.code,
            dn.status,
            dn.payment_request_id,
            dn.amount,
            from delivery_notes dn
            ),
dns_settlement_cte as (
                        select distinct dn.id,
                        s.transaction_time,
                        row_number()over(partition by dn.id order by s.transaction_time asc) as dn_settlement_index,
                        s.channel as settlement_channel,
                        s.transaction_reference,
                        s.amount as settlement_amount,
                        from delivery_notes dn, unnest(settlements) s
                        where index = 1
                        ),
dns_with_settlements_cte as (
                            select distinct date(dn.created_at) as delivery_note_creation_date,
                            dn.id as delievry_note_id,
                            dn.code as delivery_note_code,
                            dn.status as delivery_note_status,
                            dn.outlet_id as outlet_id, 
                            dn.outlet_name,
                            dn.outlet_phone_number,
                            dn.delivery_trip_id,
                            dn.amount as dn_amount,
                            dn.payment_request_id,

                            dnss.transaction_time as dn_settlement_transaction_time,
                            dnss.dn_settlement_index,
                            dnss.settlement_channel as dn_settlement_channel,
                            dnss.transaction_reference as dn_settlement_transaction_reference,
                            dnss.settlement_amount as dn_settlement_amount
                            from dns_cte dn
                            left join dns_settlement_cte dnss on dn.id = dnss.id
                            ),
dns_with_settlements_agg_cte as (
                                  select distinct delivery_note_creation_date
                                  from dns_with_settlements_cte
                                  )
                            /*,
delivery_notes_settlement_agg as (
                                  select distinct delivery_trip_id,
                                  delievry_note_id,
                                  delivery_note_code,
                                  delivery_note_created_at_index,
                                  count(distinct transaction_reference) as transaction_reference_count,
                                  string_agg(distinct settlement_channel) as settlement_channel,
                                  sum(settlement_amount) as settlement_amount
                                  from delivery_notes_with_settlements_cte
                                  group by 1,2,3,4
                                  )*/
select *
from dns_with_settlements_cte
--where country_code = 'KE'