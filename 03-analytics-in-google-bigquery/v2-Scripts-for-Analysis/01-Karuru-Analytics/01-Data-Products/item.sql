----------------------------- item -------------
with
item as (
        SELECT * 
        FROM `kyosk-prod.karuru_reports.item` 
        WHERE date(creation) > '2022-02-01'
        and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
        ),
item_cte as (
              select distinct creation,
              i.modified,
              i.bq_upload_time,
              i.company_id,
              i.id,
              i.item_code,
              i.item_name,
              i.item_group_id,
              i.maintain_stock,
              i.disabled,
              i.stock_uom,
              i.weight_uom,
              i.weight_per_unit,
              i.width,
              i.height,
              i.length,
              case when i.brand = '' then null else i.brand end as brand,
              --t.item_tax_template_id
              from item i, unnest(taxes) as t
              )
select *
--distinct weight_uom, count(distinct item_code) as item_code
--min(creation) as min_creation_datetime
from item_cte
--where item_code = 'Kings Long Grain Rice 25Kgs BAG (1 PC)'
where item_code = '210 Wheat Flour 2KG BALE (12.0 PC)'
--where item_code = 'Postman Cooking Oil 10L'