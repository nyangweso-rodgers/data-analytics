---------- Stock Entry ----------
with

erp_stock_entry as (
                            SELECT *,
                            row_number()over(partition by id order by modified_at desc) as index 
                            FROM `kyosk-prod.karuru_reports.stock_entry` 
                            WHERE date(created_at) > "2022-01-01"
                            )
select *
from erp_stock_entry
where index = 1
and date(modified_at) = '2024-03-11'