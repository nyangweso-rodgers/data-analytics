------------REVENUE REPORT--------------------------------
with
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index  
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            and date(creation) >= date_sub(date_trunc(current_date,month),interval 4 month)
                            --AND posting_date between '2022-11-01' and '2022-11-16'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
regional_mapping as (SELECT * FROM `kyosk-prod.erp_reports.upload_regional_mapping`),
warehouse_locations  as (SELECT * FROM `kyosk-prod.erp_reports.upload_warehouse_coordinates`),
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
                              dni.item_code,
                              dni.uom,
                              cast(duka_latitude as float64) as duka_latitude,
                              cast(duka_longitude as float64) as duka_longitude,
                              warehouse_latitude,
                              warehouse_longitude,
                              rm.*except(id, territory, company),
                              sum(dni.qty) as qty,
                              avg(dni.rate) as rate,
                              avg(dni.price_list_rate) as price_list_rate ,
                              sum(dni.discount_amount) as discount_amount,
                              sum(dni.amount) as amount,
                              from delivery_note_with_index dn
                              cross join unnest(items) dni
                              left join regional_mapping rm on dn.territory = rm.territory
                              left join warehouse_locations wl on dn.territory = wl.territory 
                              where index = 1
                              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
                              )
select *,
round(st_distance(ST_GEOGPOINT(warehouse_longitude,warehouse_latitude), ST_GEOGPOINT(duka_longitude, duka_latitude)),0) / 1000 as distance_in_kms
from delivery_note_with_items dn
where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE  