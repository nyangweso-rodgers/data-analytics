--------------------------- Bin -------------------------
with 
bin_with_index as (
                    SELECT *,
                    row_number()over(partition by name order by modified desc) as index 
                    FROM `kyosk-prod.erp_reports.bin` 
                    ),
bin_report as (
                select distinct date(modified) as modified,
                date(creation) as creation,
                name,
                warehouse_type,
                warehouse_territory,
                item_code,
                stock_uom,
                actual_qty,
                valuation_rate,
                stock_value,
                ordered_qty
                from bin_with_index 
                where index = 1
                )
select count(distinct name) from bin_report 
where modified = '2022-09-26'
--where FORMAT_DATE('%Y%m%d', modified) between @DS_START_DATE and @DS_END_DATE