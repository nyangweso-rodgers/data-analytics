----------- karuru ---------
--------- purchase order ----------
with
karuru_purchase_order as (
                          SELECT *,
                          row_number()over(partition by id order by modified desc) as index 
                          FROM `kyosk-prod.karuru_reports.purchase_order` 
                          WHERE date(creation) > "2022-01-01"
                          )

select distinct id, date(creation) as creation
from karuru_purchase_order
where index = 1