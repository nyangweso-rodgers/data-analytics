------------ Opening Stock Balances --------------
with
uploaded_territory_mapping as (
                      select distinct original_territory_id,
                      new_territory_id,
                      warehouse_name,
                      from `karuru_upload_tables.territory_region_mapping` 
                      ),
opening_stock_balance_scheduled_query_cte as (
                                              select distinct osb.opening_balance_date as opening_stock_balance_date,
                                              osb.company_id,
                                              osb.warehouse,
                                              osb.item_code,
                                              osb.stock_uom,
                                              round(osb.qty_after_transaction) as opening_stock_balance_qty,
                                              round(osb.valuation_rate) as opening_stock_balance_valuation_rate,
                                              round(osb.stock_value) as opening_stock_balance_value,
                                              FROM `kyosk-prod.karuru_scheduled_queries.opening_stock_balance`  osb
                                              where opening_balance_date >= date_sub(current_date, interval 14 day)
                                              and warehouse = 'Voi Main - KDKE' and item_code = 'Afia Juice Mango 500ml'
                                              --and 
                                              --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                                              --and company_id = 'KYOSK DIGITAL SERVICES LIMITED (UG)'
                                              order by warehouse, item_code, opening_stock_balance_date
                                              ),
current_date_opening_stock_balance_cte as (
                                            select *
                                            from opening_stock_balance_scheduled_query_cte
                                            where opening_balance_date = current_date
                              
                              --date_sub(date_trunc(opening_balance_date,week(monday)), interval 4 week) as four_week_demand_plan_start_date,
                              --date_sub(date_trunc(opening_balance_date,week(monday)), interval 1 day)  as four_week_demand_plan_end_date,
                              
                              
                              --utm.original_territory_id,
                              --utm.new_territory_id,
                             
                              --left join uploaded_territory_mapping utm on osb.warehouse = utm.warehouse_name
                              --where warehouse in ('Eastlands Main - KDKE', 'Embu Main - KDKE', 'Kiambu Main - KDKE', 'Kisumu 1 Main - KDKE', 'Majengo Mombasa Main - KDKE', 'Ruiru Main - KDKE', 'Voi Main - KDKE')
                              
                              --and opening_balance_date between '2024-11-12' and '2024-11-15'
                              
                              ),
stock_ledger_entry_item_lists as (
                                  select distinct warehouse,
                                  item_code,
                                  stock_uom
                                  from opening_stock_balance_scheduled_query_cte
                                  ),
get_items_with_

/*select distinct warehouse,
item_code,
stock_uom,
LAST_VALUE(
  CASE 
    WHEN opening_stock_balance_qty > 0 THEN opening_stock_balance_qty 
    ELSE NULL 
  END
  IGNORE NULLS
) OVER (
  PARTITION BY warehouse, item_code, stock_uom
  ORDER BY opening_stock_balance_date ASC 
  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) AS latest_opening_stock_balance_qty
from opening_stock_balance_cte*/
select  * from stock_ledger_entry_item_lists