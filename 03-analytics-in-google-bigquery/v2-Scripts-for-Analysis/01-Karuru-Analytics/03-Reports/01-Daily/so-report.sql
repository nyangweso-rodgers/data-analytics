------------------------------------Karuru- Sales Order Report-------------------------------------
-----------------------------------------Created By Stacey-----------------------------------------
------------------------------------Last Updated by Jimmy : 2024-04-09 Added Discount Amount and Net Total to be Amount
----------------------last updated by Stacey: ''
with 
----------------------------------------------Routes-----------------------------------------
route_mapping as (
                  SELECT distinct route_id, 
                  name as route_name
                  FROM `kyosk-prod.karuru_upload_tables.market_management_public_routes` 
                  ),
----------------------------------------Uploaded Tables------------------------------------------
regional_mapping as (
                    select distinct country,
                    region,
                    sub_region,
                    division,
                    original_territory_id,
                    new_territory_id as territory_id, 
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping` 
                      ),
new_categories_item as (
                          SELECT distinct product_bundle_id, 
                            sku_type,
                            case 
                              when country_code = 'NG' then 'Nigeria'
                              when country_code = 'KE' then 'Kenya'
                              when country_code = 'UG' then 'Uganda'
                              when country_code = 'TZ' then 'Tanzania' 
                            end as country_code,
                            FROM `kyosk-prod.karuru_upload_tables.new_categories_sku`
                            ),
item_group_mapping as (
                        SELECT distinct country_code,
                        item_group_id,
                        type
                        FROM `kyosk-prod.karuru_upload_tables.item_group_mapping` 
                        where country_code = 'KE'
                        ),   
-----------------------------------------------------Outlet Category Type---------------                                        
outlet_category_type as (
                          select *,
                          row_number()over(partition by outlet_id order by updated_at desc ) as index
                          FROM `kyosk-prod.karuru_reports.outlet_category_type`
                          where date(created_at) >= '2023-02-01'                           
                          ),
outlet_category_type_list as (
                                select distinct outlet_id, 
                                kyosk_category, 
                                outlet_group
                                from outlet_category_type
                                where index = 1
                                ),                          
------------------------------Market Activator Details--------------------------------------------------------
market_assignment_with_index as (
                                SELECT *,
                                row_number()over(partition by route_id order by valid_from desc) as index
                                FROM `kyosk-prod.karuru_reports.market_activator_assignments` 
                                WHERE date(valid_from) >= '2023-08-01' 
                              ),
market_activator_assignment as (
                                select distinct id,
                                market_activator_id,
                                route_id,
                                from market_assignment_with_index
                                where index = 1
                                and valid_from is null
                              ),
market_activator as (
                      select distinct id,
                      names,
                      email
                      from `karuru_reports.market_activators`
                      where date(created_at) >= '2023-08-01'
                    ),
market_activator_mashup as (
                            select distinct maa.market_activator_id,
                            ma.names,
                            ma.email,
                            maa.route_id,
                            from market_activator_assignment maa
                            left join market_activator ma on maa.market_activator_id = ma.id
                            ),
active_market_activator as (
                            select distinct email, 
                            names
                            from market_activator_mashup  
                            ),                            
------------------------------------Sales Order Data-----------------------------------------------
sales_order as (
                SELECT *,
                  row_number()over(partition by id order by last_modified_date desc) as index 
                  FROM `kyosk-prod.karuru_reports.sales_order` 
                  where date(created_date) >= date_sub(date_trunc(current_date(), month), interval 2 month)
                  ),
sales_order_with_items as (
                            select distinct s.territory.country_id as country_id,
                            s.territory_id,
                            date(s.created_date) as creation_date,
                            extract(HOUR from s.created_date) as creation_hour,
                            format_time("%R",time(s.created_date)) as creation_time,
                            s.delivery_window.delivery_date as delivery_date,
                            case 
                              when format_time("%R",time(s.created_date)) between '14:00' and '23:59' then 'After 2pm'
                            else 'Before 2pm' end as creation_window_dfn,
                            case 
                              when s.delivery_window.start_time = '8' then 'Morning Delivery Window'
                              when s.delivery_window.start_time = '13' then 'Afternoon Delivery Window'
                            else null end as delivery_window_definition,
                            s.name as sales_order,
                            s.id as sales_order_id,
                            s.order_status,
                            s.created_on_app,
                            s.market_developer_name,
                            s.route_id,
                            s.created_by,
                            s.territory.country_id as country,
                            s.outlet_id,
                            s.outlet.name,
                            soi.fulfilment_status,
                            soi.category_id,
                            soi.product_bundle_id,
                            soi.uom,
                            soi.catalog_item_qty as qty,
                            soi.discount_amount * soi.catalog_item_qty as discount_amount,
                            soi.net_total as amount,
                            from sales_order s, unnest(items) soi
                            where index = 1
                            --and s.order_status not in ('INITIATED','USER_CANCELLED','EXPIRED')
                            and s.order_status not in ('INITIATED')
                            ),
sales_order_report as (
                      select sowi.*except(territory_id),
                      rm.territory_id,
                      rm.region,
                      rm.sub_region,
                      rm.division,
                      r.route_name,
                      mam.names as market_activator_on_route,
                      ama.names as market_activator,
                      oct.kyosk_category,
                      oct.outlet_group,
                      coalesce(ncs.sku_type, 'GENERAL CATEGORY') as sku_category,
                      sowi.product_bundle_id = ncs.product_bundle_id as is_new_category,
                      case
                        when country in ("Nigeria") and created_on_app = 'Duka App' then  'Kyosk App'
                        when country in ("Nigeria") and created_on_app = 'AgentApp' and market_developer_name = market_activator then 'Market Activator'
                        else 'Market Developer'
                      end as created_by_Role,
                      case
                        when country in ("Nigeria") 
                      from sales_order_with_items sowi
                      left join regional_mapping rm on sowi.territory_id = rm.original_territory_id
                      left join new_categories_item as ncs on sowi.product_bundle_id = ncs.product_bundle_id and sowi.country_id = ncs.country_code
                      left join route_mapping r on sowi.route_id = r.route_id
                      left join market_activator_mashup mam on sowi.route_id = mam.route_id
                      left join active_market_activator ama on sowi.created_by =  ama.email 
                      left join outlet_category_type_list oct on sowi.outlet_id = oct.outlet_id
                      ), 
order_creation_summary as ( select distinct sales_order_id, 
                              /*case when 
                                      market_developer_name = market_activator then null
                                      else market_developer_name
                                      end as*/
                               market_developer_name,
                              case 
                                when 
                                
                                
                            from sales_order_report
                            where country = 'Nigeria'
                            ), 
sales_order_report_summary as ( select sor.*except(market_developer_name),
                                case 
                                  when sor.sales_order_id = ocs.sales_order_id then ocs.market_developer_name
                                  else sor.market_developer_name
                                end as market_developer_name,
                                from sales_order_report sor
                                left join  order_creation_summary ocs on sor.sales_order_id = ocs.sales_order_id
                                )
                                                                  

select distinct country
from sales_order_report 
where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory') 
--and FORMAT_DATE('%Y%m%d', creation_date) between @DS_START_DATE and @DS_END_DATE
--dATE(creation_date)  >= '2024-05-01'
--and market_developer_name like '%Aishat%'
--'aishatkanabe84@gmail.com'