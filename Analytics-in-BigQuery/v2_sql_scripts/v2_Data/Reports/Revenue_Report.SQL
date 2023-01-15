with
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            ),
regional_mapping as (SELECT * FROM `kyosk-prod.erp_reports.upload_regional_mapping`),
delivery_note_with_items as (
                              select distinct dn.posting_date,
                              format_time("%R", dn.posting_time) as posting_time,
                              dn.company,
                              dn.territory,
                              dn.campaign,
                              dn.sales_partner,
                              dn.kyosk_sales_order as sales_order,
                              dn.name as delivery_note,
                              dn.created_on_app,
                              dn.driver_name,
                              dn.customer,
                              dni.item_group,
                              dni.name,
                              dni.item_name,
                              dni.item_code,
                              dni.uom,
                              dni.qty,
                              dni.rate,
                              dni.price_list_rate,
                              dni.discount_amount,
                              dni.amount,
                              rm.*except(id, territory, company)
                              from delivery_note_with_index dn
                              cross join unnest(items) dni
                              left join regional_mapping rm on dn.territory = rm.territory
                              where index = 1
                              )
select * from delivery_note_with_items dn
where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE  