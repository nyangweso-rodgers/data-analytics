with
front_margins_report as (
                        SELECT 
                          distinct territory_id,
                          kyosk_delivery_note,
                          delivery_note,
                          item_code,
                          uom,
                          sum(base_net_amount) as base_net_amount,
                          sum(total_incoming_rate) as total_incoming_rate
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 2 month)
                        group by 1,2,3,4,5
                        )
select * from front_margins_report