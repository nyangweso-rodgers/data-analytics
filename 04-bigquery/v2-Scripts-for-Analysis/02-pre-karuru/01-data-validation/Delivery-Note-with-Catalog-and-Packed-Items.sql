  ----------------------- QA - DN with items --------------
  with delivery_note_with_index as (
                                    SELECT *, 
                                    row_number()over(partition by name order by modified desc) as index 
                                    FROM `kyosk-prod.erp_reports.delivery_note` 
                                    where workflow_state in ('PAID', 'DELIVERED')
                                    ),
  delivery_note_with_items as (
                                select distinct dn.company,
                                i.item_group,
                                i.item_code, 
                                i.uom,
                                i.stock_uom
                                from delivery_note_with_index dn, unnest(items) i  where index = 1
                                ),
  delivery_note_with_packed_items as (
                              select distinct dn.company,
                              pi.parent_item as catalog_name,
                              pi.item_code inventory_name
                              from delivery_note_with_index dn, unnest(packed_items) pi  where index = 1
                              )
  select dnwpi.*, dnwi.uom as catalog_uom, dnwi.stock_uom, dnwi.item_group from delivery_note_with_packed_items dnwpi
  left join delivery_note_with_items dnwi on dnwpi.company = dnwi.company and dnwpi.catalog_name = dnwi.item_code
  --where item_group = 'Diapers'
  order by 2,3,1