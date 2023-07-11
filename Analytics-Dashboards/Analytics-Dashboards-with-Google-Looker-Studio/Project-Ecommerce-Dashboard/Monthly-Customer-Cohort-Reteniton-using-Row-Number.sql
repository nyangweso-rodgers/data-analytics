---------------------- Query for Month on Month Cohort Retention using Row Number Function  ----------------------
with
online_retail as (
                    SELECT distinct date(InvoiceDate) as InvoiceDate,
                    --Country,
                    SAFE_CAST(CustomerID as string) as CustomerID,
                    --SAFE_CAST(InvoiceNo as string) as InvoiceNo,
                    --SAFE_CAST(StockCode as string) as StockCode,
                    --Description,
                    --Quantity,
                    --UnitPrice,
                    Quantity * UnitPrice as amount
                    FROM `general-364419.table_uploads.online_retail`
                    where Quantity > 0
                    and CustomerID is not null -- remove rows with null customer ids
                    ),

monthly_transactions as (
                          select distinct CustomerID, 
                          date_trunc(InvoiceDate, month) as InvoiceMonth 
                          from online_retail
                          ),
monthly_transactions_with_row_number as (
                                          select *, 
                                          row_number()over(partition by CustomerID order by InvoiceMonth asc) as InvoiceMonthIndex
                                          from monthly_transactions
                                          ),
monthly_cohort_retention_list as (
                                    select distinct a.CustomerID,
                                    a.InvoiceMonth as FirstInvoiceMonth,
                                    b.InvoiceMonth,
                                    date_diff(b.InvoiceMonth, a.InvoiceMonth, month) as MonthsAfterConversion
                                    from monthly_transactions_with_row_number a
                                    join (select CustomerID, InvoiceMonth from monthly_transactions_with_row_number) b using(CustomerID)
                                    where a.InvoiceMonthIndex = 1
                                    order by 1,2
                                    ),
monthly_cohort_retention_count as (
                                    select distinct FirstInvoiceMonth,
                                    MonthsAfterConversion,
                                    count(distinct CustomerID) as Customers
                                    from monthly_cohort_retention_list
                                    group by 1,2
                                    )
select * from monthly_cohort_retention_count