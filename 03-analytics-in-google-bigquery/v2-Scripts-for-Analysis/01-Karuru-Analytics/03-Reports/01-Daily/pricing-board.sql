---------------- Karuru ------------
---------- Pricing Guide -----------
with
------------------ Front Margins -----------------------
karuru_front_margins as (
                          SELECT distinct creation_date,
                          territory_id,
                          item_group_id,
                          item_code,
                          item_name_of_packed_item,
                          sum(qty_of_sales_invoice) as qty_of_sales_invoice,
                          sum(qty_of_packed_item) as qty_of_packed_item,
                          sum(base_amount) as base_amount,
                          sum(base_amount) / sum(qty_of_sales_invoice) as unit_sp_vat_incl,
                          sum(base_net_amount) as base_net_amount,
                          sum(incoming_rate) as incoming_rate,
                          sum(total_incoming_rate) as total_incoming_rate,
                          round(sum(base_net_amount) - sum(total_incoming_rate),2) as front_margins_vat_excl,
                          1 - (sum(total_incoming_rate) / sum(base_net_amount)) as front_margins_percent
                          FROM `kyosk-prod.karuru_scheduled_queries.front_margin` 
                          WHERE creation_date >= date_sub(date_trunc(current_date, month), interval 1 month)
                          and creation_date between '2024-05-05'  and "2024-05-11" 
                          --and FORMAT_DATE('%Y%m%d', creation_date) between @DS_START_DATE and @DS_END_DATE
                          group by 1,2,3,4,5
                          --order by creation_date desc, territory_id, item_code
                          ),
front_margins_agg as (
                      select distinct --country,
                      territory_id,
                      item_group_id,
                      item_code,
                      item_name_of_packed_item,
                      sum(qty_of_sales_invoice) as qty_of_sales_invoice,
                      sum(qty_of_packed_item) as qty_of_packed_item,
                      sum(base_amount) as base_amount,
                      sum(base_amount) / sum(qty_of_sales_invoice) as avg_unit_sp_vat_incl,
                      1 - (sum(total_incoming_rate) / sum(base_net_amount)) as front_margins_percent,
                      sum(total_incoming_rate) / sum(qty_of_sales_invoice) as avg_unit_cp_vat_excl,
                      round(sum(base_amount)/sum(base_net_amount)-1,2) as calculated_vat_rate,
                      from karuru_front_margins
                      group by 1,2,3,4
                      ),                 
latest_front_margins_prices_report as (
                                      select distinct territory_id,
                                      item_code,
                                      LAST_VALUE(creation_date) 
                                        OVER (PARTITION BY territory_id, item_code ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_creation_date,
                                      LAST_VALUE(unit_sp_vat_incl) 
                                        OVER (PARTITION BY territory_id, item_code ORDER BY creation_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_unit_sp_vat_incl,
                                      from karuru_front_margins
                                      ),
---------------------- Latest Purchase Receipts --------------------------------
purchase_receipts as (
                      SELECT *,
                      row_number()over(partition by id order by date_modified desc) as index
                      FROM `kyosk-prod.karuru_reports.purchase_receipt` 
                      WHERE date(date_created) >= date_sub(date_trunc(current_date, month), interval 2 month)
                      ),
purchase_receipts_report as (
                            select distinct posting_date,
                            territory_id,
                            pri.item_code,
                            pri.item_name,
                            avg(rate) as rate,
                            coalesce(safe_divide(sum(amount) , sum(received_qty)),0) as calculated_unit_pr_rate
                            from purchase_receipts pr, unnest(items) as pri
                            where index = 1
                            and buying_type in ('PURCHASING')
                            and workflow_state in ('COMPLETED')
                            and company_id in ('KYOSK DIGITAL SERVICES LIMITED (UG)', 'KYOSK DIGITAL SERVICES LIMITED (TZ)')
                            group by 1,2,3,4
                            ),
latest_purchase_receipt_report as (
                                  select distinct territory_id,
                                  item_code,
                                  LAST_VALUE(posting_date) 
                                    OVER (PARTITION BY territory_id, item_code ORDER BY posting_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_purchase_receipt_posting_date,
                                  LAST_VALUE(calculated_unit_pr_rate) 
                                    OVER (PARTITION BY territory_id, item_code ORDER BY posting_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS latest_purchase_receipt_rate
                                  from purchase_receipts_report
                                  ),
---------------- Data from Apphseets ----------------------
front_margin_targets as (
                      select distinct product_item_code, 
                      start_date, 
                      end_date, 
                      UPPER(country_name) as country_name, 
                      territory_name, 
                      AVG(target) as target 
                      from `kyosk-prod.appsheet_data.front_margin_targets`
                      group by 1,2,3,4,5
                      ),   
csv_uploaded_prices as (
                        SELECT cpu.*  except (id,Batch_Number,Country_Id, Product_Id) 
                        FROM `kyosk-prod.appsheet_data.competitor_pricing_items_uploads_temp` cpu 
                        where Competitor_Price is not null
                        ),
competitor_pricing_item as (
                            SELECT distinct created_at,
                            pricing_date,
                            kyosk_product_id,
                            pricing_id,
                            price,
                            proposed_price,
                            target_margin,
                            created_by
                            FROM `kyosk-prod.appsheet_data.competitor_pricing_item` 
                            where is_kyosk_product = true
                            ),
catalog_products as (
                      SELECT distinct id,
                      catalog_name,
                      product_item_code,
                      catalog_uom
                      FROM `kyosk-prod.appsheet_data.catalog_products`
                      ),
competitor_pricing as (
                        SELECT distinct id,
                        country_id,
                        territory_id,
                        competitor_id,
                        md_name
                        FROM `kyosk-prod.appsheet_data.competitor_pricing`
                        ),
competitors as (
                  select distinct id,
                  competitor_name,
                  competitor_type
                  from `kyosk-prod.appsheet_data.competitors`
                  ),
countries as (
                select distinct id,
                country, 
                --company
                FROM `kyosk-prod.appsheet_data.countries`
                ),
territories as (
                  select distinct territory_id,
                  territory
                  from `kyosk-prod.appsheet_data.territories`
                  ),
competitor_pricing_report as (
                              select distinct created_by as price_created_by,
                              created_at,
                              pricing_date,
                              countries.country as Country,
                              territories.territory as Territory,
                              competitors.competitor_name as Competitor,
                              '' as Product_Category,
                              catalog_products.Catalog_name,
                              '' as Cataloge_UoM,
                              
                              price as Competitor_Price,
                              case
                                when created_by like '%@kyosk.app' then proposed_price else 0 
                              end as Proposed_Price,
                              target_margin   as Target_Margin 
                              from competitor_pricing_item
                              join catalog_products on competitor_pricing_item.kyosk_product_id = catalog_products.id
                              left join competitor_pricing on competitor_pricing_item.pricing_id = competitor_pricing.id
                              left join competitors on competitor_pricing.competitor_id = competitors.id
                              left join countries on competitor_pricing.country_id = countries.id
                              left join territories on competitor_pricing.territory_id =  territories.territory_id
                              ),
latest_competitor_pricing_report as (
                                    select distinct Catalog_name, 
                                    Competitor, 
                                    territory, 
                                    Country as country,
                                    LAST_VALUE(pricing_date) 
                                    OVER (PARTITION BY territory, Catalog_name ORDER BY created_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS competitor_pricing_date,
                                    avg(Competitor_price)over(partition by territory, Catalog_name) as competitor_price,
                                    avg(proposed_price)over(partition by territory, Catalog_name) as proposed_price
                                    --max(pricing_date) as competitor_pricing_date, 
                                    --max(Competitor_price) as competitor_price, 
                                    --min(proposed_price) as proposed_price 
                                    from competitor_pricing_report
                                    --group by 1,2,3,4
                                    ),
------------------------ 
mashup as (
            select fma.*,
            lcpr.competitor_pricing_date,
            fma.avg_unit_cp_vat_excl * (calculated_vat_rate + 1) as avg_unit_cp_vat_incl,
            lfmpr.latest_unit_sp_vat_incl,
            lprr.latest_purchase_receipt_posting_date,
            lprr.latest_purchase_receipt_rate,
            lprr.latest_purchase_receipt_rate / fma.qty_of_sales_invoice as latest_purchase_receipt_unit_rate,
            lcpr.competitor_price,
            lcpr.proposed_price,  
            lcpr.country as Country,          
            1- (SAFE_DIVIDE	((fma.avg_unit_cp_vat_excl * (calculated_vat_rate + 1)),lcpr.proposed_price)) as implied_fron_margin,
            fmt.target as front_margins_target 
            from front_margins_agg fma
            left join latest_front_margins_prices_report lfmpr on fma.territory_id = lfmpr.territory_id and fma.item_code = lfmpr.item_code
            left join latest_purchase_receipt_report lprr on fma.territory_id = lprr.territory_id and fma.item_name_of_packed_item = lprr.item_code
            left join latest_competitor_pricing_report lcpr on fma.territory_id = lcpr.Territory and fma.item_code = lcpr.Catalog_name
            left join front_margin_targets fmt on lcpr.country = fmt.country_name and fma.item_code = fmt.product_item_code 
            ),
pricing_recommendation_report as (
                                  select *,
                                  case
                                    when proposed_price > latest_unit_sp_vat_incl then 'YES'
                                    when proposed_price < latest_unit_sp_vat_incl then 'YES'
                                    when proposed_price = latest_unit_sp_vat_incl then 'NO'
                                  else null end as price_change
                                  from mashup
                                  )
select prr.*,
from latest_competitor_pricing_report prr
where territory in ('Kiambu')
and item_group_id in ('Cooking Oil')
and item_code in ('Postman Cooking Oil 10L JERICAN (1.0 PC)')