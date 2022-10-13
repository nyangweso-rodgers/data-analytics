with 
sales_order_with_index as (
                            select *,
                            row_number()over(partition by name order by modified desc) as index
                            from `kyosk-prod.erp_reports.sales_order`
                            where created_on_app = 'Duka App'
                            ),
customer_with_sales_partner as 
                                (                            
                                  select *, row_number()over(partition by customer order by transaction_date desc) as sales_partner_index
                                  from 
                                  (select distinct customer, sales_partner, territory, transaction_date from sales_order_with_index where index = 1)
                                  )
select distinct customer, sales_partner, territory from customer_with_sales_partner where sales_partner_index = 1