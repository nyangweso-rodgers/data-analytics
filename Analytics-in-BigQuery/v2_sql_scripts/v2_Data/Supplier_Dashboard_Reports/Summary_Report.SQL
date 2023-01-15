---------------- Supplier Dashboard : Summary ----------------------------------
with
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and workflow_state in ('PAID', 'DELIVERED')
                            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and territory not in ('Kyosk HQ')
                            ),
regional_mapping as (SELECT * FROM `kyosk-prod.erp_reports.upload_regional_mapping`),
supplier_product_mapping as (
                              select distinct item_name_of_delivery_name as item_name, 
                              item_supplied_by, 
                              item_brand_name
                              from `kyosk-prod.erp_reports.upload_supplier_product_mapping_v2` ),
delivery_note_with_items as (
                              select distinct dn.posting_date,
                              dn.territory,
                              dn.customer,
                              dn.name as delivery_note,
                              dni.item_group,
                              dni.item_code as item_name,
                              dni.uom,
                              sum(dni.qty) as qty,
                              sum(dni.base_amount) as base_amount,
                              spm.item_supplied_by,
                              spm.item_brand_name,
                              rm.sub_region,
                              from delivery_note_with_index dn, unnest(items) dni
                              left join regional_mapping rm on dn.territory = rm.territory
                              left join supplier_product_mapping spm on dni.item_code = spm.item_name
                              where index = 1
                              and dni.item_group in ('Maize Flour', 'All Purpose Flour', 'Wheat Flour')
                              group by 1,2,3,4,5,6,7,10,11,12
                              )
select * from delivery_note_with_items  
where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE
order by 1,2,3