with
purchase_invoice as (
                    SELECT *,
                    row_number()over(partition by id order by date_modified desc) as index 
                    FROM `kyosk-prod.karuru_reports.purchase_invoice` 
                    --WHERE date(date_created) > "2022-10-01"
                    WHERE date_trunc(date(date_created),month) = '2024-03-01'
                    )
select distinct date(date_created) as date_created,
posting_date,
id, 
workflow_state,
status,
buying_type,
supplier,
supplier_name,
company_id,
set_warehouse_id,
i.territory_id,
i.purchase_order,
i.purchase_receipt_id,
i.item_id,
i.uom,
i.brand,
i.item_group_id,
i.received_qty,
i.rejected_qty,
i.discount_amount,
i.amount,
from purchase_invoice pi, unnest(items) i
where index = 1
