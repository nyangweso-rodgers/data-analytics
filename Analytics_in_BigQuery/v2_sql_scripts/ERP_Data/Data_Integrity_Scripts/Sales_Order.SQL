---------------------------- Sales Order --------------------
with
sales_order_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.sales_order` 
                            --WHERE transaction_date  between '2022-10-01'  and '2022-10-02'
                            ),

sales_order_summary as (select distinct transaction_date, count(distinct name) from sales_order_with_index where index = 1  group by 1 order by 1),
sales_order_lists as (select distinct transaction_date, name, grand_total from sales_order_with_index where index = 1 order by 1,2 ),

recent_data as (
                select *, row_number()over(partition by customer order by transaction_date desc) as sales_partner_index
                from
                (select distinct transaction_date, customer,sales_partner  from sales_order_with_index where index = 1)
                ) 
select distinct customer, sales_partner from recent_data where sales_partner_index = 1