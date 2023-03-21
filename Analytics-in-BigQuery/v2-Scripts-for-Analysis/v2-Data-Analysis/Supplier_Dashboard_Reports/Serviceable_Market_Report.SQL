------------------------------- Supplier Dashboard - Serviceable Market Report ----------------------
with
regional_mapping as (SELECT * FROM `kyosk-prod.erp_reports.upload_regional_mapping`),
supplier_product_mapping as (
                              select distinct  item_supplied_by, item_name 
                              from `kyosk-prod.erp_reports.upload_supplier_product_mapping` 
                              ),
customer_with_index as (
                        SELECT *,
                        row_number()over(partition by name order by modified desc) as index 
                        FROM `kyosk-prod.erp_reports.customer` 
                        where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                        ),
monthly_customer_registration as (
                                  SELECT date_trunc(date(creation), month) as registration_month, 
                                  rm.sub_region,
                                  count(distinct name) as monthly_registered
                                  FROM customer_with_index  cu
                                  left join regional_mapping rm on cu.territory = rm.territory
                                  where index = 1
                                  and cu.territory not in ('Kyosk HQ', 'DKasarani') and sub_region is not null
                                  group by 1,2
                                  ),
running_monthly_customer_registrations as (
                                            select *,
                                            sum(monthly_registered)over(partition by sub_region order by sub_region, registration_month) as total_registered
                                            from monthly_customer_registration
                                            ),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and territory not in ('Kyosk HQ')
                            and workflow_state in ('PAID', 'DELIVERED')
                            ),
delivery_note_with_items as (
                            select distinct date_trunc(dn.posting_date, month) as posting_month,
                            item_supplied_by,
                            count(distinct customer) as count_of_active_customers
                            from delivery_note_with_index dn, unnest(items) dni
                            left join supplier_product_mapping spm on dni.item_code = spm.item_name
                            where index = 1 and item_supplied_by is not null
                            group by 1,2
                            order by 1,2
                            ),
report as (
            select rmcr.*, dnwi.*except(posting_month),
            count_of_active_customers / total_registered as percent_of_served_market
            from running_monthly_customer_registrations rmcr
            left join delivery_note_with_items dnwi on rmcr.registration_month = dnwi.posting_month
            )
select * from report
--where registration_month = '2022-02-01'
order by 2,1