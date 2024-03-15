---------- Stock Ledger Entry ----------
with
vars AS (
  SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  --SELECT DATE '2024-03-10' as current_start_date, DATE '2024-03-10' as current_end_date ),
erp_stock_ledger_entry as (
                            SELECT *,
                            --row_number()over(partition by id order by modified desc) as index 
                            FROM `kyosk-prod.karuru_reports.stock_ledger_entry` 
                            WHERE date(creation) > "2022-01-01"
                            ),
stock_ledger_entry_report as (
                              select distinct company_id, 
                              warehouse, 
                              item_code,
                              stock_uom, 
                              modified,
                              row_number()over(partition by warehouse, item_code, stock_uom order by modified desc) as transaction_index,
                              qty_after_transaction,
                              valuation_rate
                              from erp_stock_ledger_entry, vars
                              where date(posting_datetime) between current_start_date and current_end_date
                              and is_cancelled = false
                              )
select *
from stock_ledger_entry_report
where warehouse = 'Ruiru Main - KDKE'
--and date(posting_datetime) = '2024-03-10'
--AND item_code = '210 Maize Flour 2KG'
order by warehouse, item_code,stock_uom, modified desc