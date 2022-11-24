------------------ v3 Gross and Net Profit from DNs -----------------------
with
currency_conversion_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_currency_conversion_v2` where ex_rate not in ('BUDGET RATE (2022)') ),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            and date(creation) >= date_sub(date_trunc(current_date,month),interval 2 month)
                            and posting_date >= '2022-11-16'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
delivery_note_with_items as (
                              select distinct dn.posting_date,
                              dn.company,
                              case 
                                  when dn.company = 'KYOSK DIGITAL SERVICES LTD (KE)' then 'KENYA'
                                  when dn.company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then 'UGANDA'
                                  when dn.company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then 'TANZANIA'
                                  when dn.company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' then 'NIGERIA'
                              else null end as country,
                              dn.territory,
                              dn.name as delivery_note,
                              dn.kyosk_sales_order as sales_order,
                              dn.workflow_state,
                              dni.item_group,
                              dni.name,
                              dni.item_code,
                              dni.uom,
                              dn.campaign,
                              dni.item_tax_rate,
                              dni.qty,
                              --dni.base_rate,
                              --dni.base_net_rate,
                              dni.discount_amount,
                              dni.base_amount,
                              dni.base_net_amount,
                              dni.base_amount - dni.base_net_amount as variance_amount,
                              round((dni.base_amount / dni.base_net_amount - 1) * 100) as calculated_vat_rate_percent
                              from delivery_note_with_index dn, unnest(items) dni
                              where index = 1
                              ),
delivery_note_with_packed_items as (
                                      select distinct parent_detail_docname,
                                      pi.item_code,
                                      pi.uom,
                                      pi.incoming_rate,
                                      pi.qty * pi.incoming_rate as total_incoming_rate,
                                      from delivery_note_with_index dn, unnest(packed_items) pi
                                      ),
gross_margin_report as (
                          select dnwi.*,
                          dnwpi.item_code as item_code_of_packed_item,
                          dnwpi.uom as uom_of_packed_item,
                          dnwpi.incoming_rate,
                          dnwpi.total_incoming_rate,
                          ccd.ex_rate_amount,
                          dnwpi.total_incoming_rate / ccd.ex_rate_amount as total_incoming_rate_in_usd,
                          dnwi.base_amount / ccd.ex_rate_amount as base_amount_in_usd,
                          dnwi.base_net_amount / ccd.ex_rate_amount as base_net_amount_in_usd
                          --/ ccd.ex_rate_amount
                          from delivery_note_with_items dnwi
                          left join delivery_note_with_packed_items dnwpi on dnwi.name = dnwpi.parent_detail_docname
                          left join currency_conversion_data ccd on dnwi.company = ccd.company and dnwi.posting_date between ccd.start_date and ccd.end_date 
                          )
select * from gross_margin_report
where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE