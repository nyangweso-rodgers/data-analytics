---------- Stock Ledger Entry ----------
with
vars AS (
  --SELECT PARSE_DATE('%Y%m%d', @DS_START_DATE) as current_start_date, PARSE_DATE('%Y%m%d', @DS_END_DATE) as current_end_date ),
  SELECT DATE '2024-03-10' as current_start_date, DATE '2024-04-25' as current_end_date ),
stock_ledger_entry as (
                            SELECT *,
                            --row_number()over(partition by id order by modified desc) as index 
                            FROM `kyosk-prod.karuru_reports.stock_ledger_entry` 
                            --WHERE date(creation) > "2022-01-01"
                            WHERE date(creation) > '2024-11-01'
                            ),
stock_ledger_entry_report as (
                              select distinct creation,
                              bq_upload_time,
                              posting_datetime,
                              modified,
                              company_id, 
                              warehouse, 
                              name,
                              voucher_no,
                              voucher_type,
                              item_code,
                              stock_uom, 
                              row_number()over(partition by warehouse, item_code, stock_uom order by modified desc) as transaction_index,
                              qty_after_transaction,
                              valuation_rate,
                              actual_qty
                              from stock_ledger_entry--, vars
                              --where date(posting_datetime) between current_start_date and current_end_date
                              --and is_cancelled = false
                              )
select *
--max(creation) as max_creation, max(modified) as max_modified, max(bq_upload_time) as max_bq_upload_time, max(posting_datetime) as max_posting_datetime
from stock_ledger_entry_report
--where warehouse like 'Mwenge%'
--and date(posting_datetime) = '2024-03-10'
--and item_code = 'Azania King limau Washing Powder 3Kg'
--and voucher_type = "Purchase Receipt"
--order by warehouse, item_code,stock_uom, modified desc
where posting_datetime = '2024-11-23'