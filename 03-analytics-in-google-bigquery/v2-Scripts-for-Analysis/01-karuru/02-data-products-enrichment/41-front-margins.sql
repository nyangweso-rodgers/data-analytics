----------- front margins scheduled query ----------------
with
front_margins_cte as (
                        SELECT distinct creation_date,
                        delivery_date,
                        company,
                        territory_id,
                        route_id,
                        route_name,
                        sales_invoice,
                        kyosk_delivery_note as delivery_note_id,
                        delivery_note,
                        delivery_trip_id,
                        delivery_trip_code,
                        outlet_id,

                        item_group_id,
                        item_code,
                        uom,
                        item_name_of_packed_item,
                        uom_of_packed_item,
                        tax_rate,
                        qty_of_packed_item,
                        incoming_rate,
                        total_incoming_rate,
                        base_amount,
                        base_net_amount
                        FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                        wHERE delivery_date > date_sub(date_trunc(current_date, month), interval 1 month)
                        --where delivery_date = date_sub(current_date, interval 1 day)
                        --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                        --and kyosk_delivery_note = '0H6VF7TDZXPQ3'
                        and territory_id in ('Ruiru')
                        and delivery_note = 'DN-RUIR-0H7FBSGX7XMRD'
                        ),
catalog_items_front_margins_cte as (
                                select distinct creation_date,
                                company,
                                territory_id,
                                delivery_note_id,
                                item_code,
                                uom,
                                tax_rate,
                                round(sum(base_amount),0) as gmv_vat_incl,
                                round(sum(base_net_amount),0) as gmv_vat_excl,
                                round(sum(total_incoming_rate),0) as avg_cost_vat_excl,
                                round(sum(total_incoming_rate) /sum(qty_of_packed_item),1) as unit_avg_cost_vat_excl,
                                from front_margins_cte
                                group by 1,2,3,4,5,6,7
                                ),
latest_catalog_front_margins_cte as ( 
select distinct territory_id,
item_code,
uom,
LAST_VALUE(date(creation_date)) OVER (PARTITION BY territory_id, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_creation_date,
LAST_VALUE(tax_rate) OVER (PARTITION BY territory_id, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_tax_rate,
LAST_VALUE(unit_avg_cost_vat_excl) OVER (PARTITION BY company, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_country_unit_avg_cost_vat_excl,
LAST_VALUE(unit_avg_cost_vat_excl) OVER (PARTITION BY territory_id, item_code, uom ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as latest_territory_unit_avg_cost_vat_excl
from catalog_items_front_margins_cte
)
select *
--distinct item_name_of_packed_item, count(distinct item_code) as item_code, string_agg(distinct item_code, "/") group by 1 having item_code > 1
from front_margins_cte
