----------------------------------- Test Script ------------------
---------------------------------- Sales Commissions -------------------
with
--------------------------- Targets -------------------------------------------
targets as (
            SELECT distinct start_date,
            end_date,
            sales_partner,
            territory,
            revenue_target,
            dukas_target,
            sku_target,
            lppc_target
            FROM `kyosk-prod.uploaded_tables.upload_sales_partner_targets` 
            ),
----------------------------------------------- Sales -----------------------------
sales_commissions_data as (
                            select distinct date_trunc(posting_date, month) as posting_month,
                            territory,
                            case 
                              when sales_partner = 'delinah cherera' then 'Delinah Cherera' else sales_partner 
                            end as sales_partner,
                            customer,
                            item_group,
                            item_code,
                            name,
                            sum(amount) as amount
                            from `kyosk-prod.erp_sales_commissions.erp_sales_commission`
                            where posting_date between '2023-03-01' and '2023-03-31'
                            group by 1,2,3,4,5,6,7
                            ),
 ------------------------------------------------- Assortment --------------------------------------
monthly_assortment_data as (
                            select distinct posting_month,
                            sales_partner,
                            territory,
                            item_code,
                            count(distinct name) as sku_units
                            from sales_commissions_data
                            group by 1,2,3,4
                            having sku_units >= 20
                            ),
monthly_assortment_target as (
                            select distinct posting_month,
                            territory,
                            count(distinct item_code) as target_skus
                            from monthly_assortment_data
                            group by 1,2
                            ),
monthly_assortment_mashup as (
                              select distinct a.posting_month,
                              a.sales_partner,
                              a.territory,
                              count(distinct a.item_code) as unique_skus,
                              b.target_skus
                              from monthly_assortment_data a
                              left join monthly_assortment_target b on a.posting_month = b.posting_month and a.territory = b.territory
                              group by 1,2,3,5
                              order by 3,2
                              ),
------------------------------------------------- Summary ----------------------------------------
monthly_sales_with_targets as (
                                select distinct a.posting_month,
                                a.territory,
                                a.sales_partner,
                                t.dukas_target,
                                count(distinct customer) as count_of_customers,
                                t.revenue_target,
                                sum(amount) as amount,
                                t.lppc_target,
                                round(count(item_group) / count(distinct name), 1) as avg_item_group_per_dn,
                                from sales_commissions_data a
                                left join targets t on a.sales_partner = t.sales_partner and a.territory = t.territory and a.posting_month = t.start_date
                                left join monthly_assortment_data b on a.sales_partner = b.sales_partner and a.territory = b.territory and a.posting_month = b.posting_month
                                group by 1,2,3,4,6,8
                              ),
sales_with_performance as (
                            select mswt.*,
                            round(count_of_customers / dukas_target, 2) as percent_performance_duka_count,
                            round(avg_item_group_per_dn / lppc_target, 2) as percent_performance_lppc,
                            round(amount / revenue_target, 2) as percent_performance_revenue,
                            c.target_skus,
                            c.unique_skus,
                            round(unique_skus / target_skus, 2) as percent_performance_sku
                            from monthly_sales_with_targets mswt
                            left join monthly_assortment_mashup c on mswt.territory = c.territory and  mswt.posting_month = c.posting_month and mswt.sales_partner = c.sales_partner
                            ),
model as (
            select *,
            from sales_with_performance
            )
----------------------------- targets -------------------------

-------------------------------- model -----------------------

select * from sales_with_performance
where sales_partner in ('Gladys Mumo', 'Stacy Makena', 'patrick mbogho')