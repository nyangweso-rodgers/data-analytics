----------------------------- Front Margins - v2 - from Sales Invoices --------------------------
-------------------------------- Created By: Rodgers ------------------------------------
with
sales_invoice_with_index as (
                              SELECT *, 
                              row_number()over(partition by name order by modified desc) as index 
                              FROM `kyosk-prod.erp_reports.sales_invoice` 
                              where date(creation) >= '2023-01-01'
                              and  territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                              --and name = "SI-KARA-7SWE-23"
                              ),
delivery_note_with_index as (
                            SELECT *, row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note`
                            ),
sales_invoice_with_items as (
                              select distinct date(si.creation) as creation_date,
                              si.company,
                              si.territory,
                              si.name as sales_invoice,
                              sii.delivery_note,
                              sii.dn_detail,
                              sii.item_code,
                              sii.uom,
                              sii.item_group,
                              sii.item_tax_template,
                              sii.qty as qty_of_sales_invoice,
                              sii.discount_amount as discount_amount_of_sales_invoice,
                              sii.base_amount as base_amount_of_sales_invoice,
                              sii.base_net_amount as base_net_amount_of_sales_invoice
                              from sales_invoice_with_index si, unnest(items) sii
                              where index = 1
                              ),
delivery_note_with_packed_items as (
                                    select distinct pi.parent_detail_docname,
                                    pi.item_name as item_name_of_packed_item,
                                    pi.uom as uom_of_packed_item,
                                    pi.incoming_rate,
                                    pi.qty as qty_of_packed_item,
                                    pi.qty * pi.incoming_rate as total_incoming_rate
                                    from delivery_note_with_index dn,unnest(packed_items) pi
                                    where index = 1
                                    order by 1
                                    ),
front_margin_report as (
                          select si.*,
                          dnwpi.item_name_of_packed_item,
                          dnwpi.uom_of_packed_item,
                          qty_of_packed_item,
                          incoming_rate,
                          total_incoming_rate
                          from sales_invoice_with_items si
                          left join delivery_note_with_packed_items dnwpi on si.dn_detail = dnwpi.parent_detail_docname
                          )
select * from front_margin_report
--where incoming_rate = 0
--where creation_date between '2023-04-01' and '2023-04-30'
--and company = 'KYOSK DIGITAL SERVICES LTD (KE)'

