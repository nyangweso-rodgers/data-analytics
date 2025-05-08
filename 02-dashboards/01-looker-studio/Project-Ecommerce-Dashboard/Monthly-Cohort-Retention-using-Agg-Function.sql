--------------------------Query for Monthly Cohort Retention using Agg Function ----------------------
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
                          ),
joining_month as (
                  select min(InvoiceMonth) as CohortMonthYear,
                  CustomerID
                  from monthly_transactions
                  group by 2
                  ),
-- find the size of each cohort by by counting the number of unique stalls that show up for the first time in a month
cohort_size as (
                  select extract(year from CohortMonthYear) as CohortYear,
                  extract(month from CohortMonthYear) as CohortMonth,
                  count(1) as CustomersCount
                  from joining_month
                  group by 1,2
                  order by 1,2
                  ),
mashup as (
            select date_diff(mt.InvoiceMonth, jm.CohortMonthYear, month) as MonthNumber,
            mt.CustomerID
            from monthly_transactions mt
            left join joining_month jm on mt.CustomerID = jm.CustomerID
            order by 1 desc
            ),
retention_table as (
                      select extract(year from jm.CohortMonthYear) as CohortYear,
                      extract(month from jm.CohortMonthYear) as CohortMonth,
                      m.MonthNumber,
                      count(1) as CustomersCount
                      from mashup m
                      left join joining_month jm on m.CustomerID = jm.CustomerID
                      group by 1,2,3
                      order by 1,2,3
                      )
select rt.CohortYear,
rt.CohortMonth,
rt.MonthNumber,
rt.CustomersCount,
rt.CustomersCount / cs.CustomersCount as percent
from retention_table rt
left join cohort_size cs on rt.CohortYear = cs.CohortYear and rt.CohortMonth = cs.CohortMonth
order by 1,2,3