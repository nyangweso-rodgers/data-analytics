------------------------ Month on Month Customer Retention Analysis ------------------
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
                    --Quantity * UnitPrice as amount
                    FROM `general-364419.table_uploads.online_retail`
                    where Quantity > 0
                    and CustomerID is not null -- remove rows with null customer ids
                    ),
monthly_transactions as (
                          select distinct CustomerID, 
                          date_trunc(InvoiceDate, month) as InvoiceMonth 
                          from online_retail
                          )
select date_add(last_month.InvoiceMonth, interval 1 month) as InvoiceMonth,
count(distinct last_month.CustomerID) as ActiveCustomers,
count(distinct this_month.CustomerID) as RetainedCustomers,
safe_divide(count(distinct this_month.CustomerID), coalesce(count(distinct last_month.CustomerID), null)) as RetentionPercent
from monthly_transactions as last_month
left join monthly_transactions as this_month on last_month.CustomerID = this_month.CustomerID and this_month.InvoiceMonth = date_add(last_month.InvoiceMonth, interval 1 month)
group by 1
order by 1