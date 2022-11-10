with
planned_revenue_data as  (SELECT * FROM `kyosk-prod.erp_reports.upload_planned_revenue_v2` order by 1) ,
currency_conversion_data as (SELECT * FROM `kyosk-prod.erp_reports.upload_currency_conversion_v2` where ex_rate not in ('BUDGET RATE (2022)') ),
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID', 'DELIVERED')
                            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and posting_date between '2022-09-01' and '2022-09-30'
                            --and kyosk_sales_order = 'SAL-ORD-ATH7LUB'
                            ),
delivery_note_report as (
                            select distinct dnwi.posting_date,
                            dnwi.company,
                            dnwi.customer,
                            dnwi.kyosk_sales_order as sales_order,
                            dnwi.created_on_app,
                            dnwi.grand_total,
                            dnwi.grand_total / ccd.ex_rate_amount as grand_total_in_usd
                            from delivery_note_with_index dnwi
                            left join currency_conversion_data ccd on dnwi.company = ccd.company and dnwi.posting_date between ccd.start_date and ccd.end_date      
                            where index = 1
                            )
select sum(grand_total), sum(grand_total_in_usd) from delivery_note_report