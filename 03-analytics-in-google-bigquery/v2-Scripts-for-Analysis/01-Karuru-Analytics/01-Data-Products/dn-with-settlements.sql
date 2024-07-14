------------ DNs =with Settlement ----------
with
delivery_notes as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                --where date_trunc(date(created_at),month) >= date_sub(date_trunc(current_date, month), interval 3 month)
                ),
delivery_notes_with_settlement as (
                                    select distinct created_at,
                                    --date(created_at) as dn_created_at,
                                    country_code,
                                    id,
                                    code,
                                    dn.status as dn_status,
                                    dn.outlet_id,
                                    outlet.name as outlet_name,
                                    outlet.phone_number,
                                    dn.payment_request_id,
                                    s.channel as settlement_channel,
                                    transaction_reference,
                                    s.amount as settlement_amount,
                                    from delivery_notes dn, unnest(settlements) s
                                    --where index = 1
                                    --and s.status not in ('INITIATED')
                                    
                                    )
select distinct --created_at, 
id,country_code, code, --count(distinct settlement_channel) as count_settlement_channel,
--string_agg(distinct settlement_channel, "/" order by settlement_channel) as settlement_channel
settlement_channel,
from delivery_notes_with_settlement
where country_code = 'KE'
--where code = 'DN-KARA-0FWK97MDPQNST'
--where code =  "DN-VOIM-0G1SC53CPNPR5"