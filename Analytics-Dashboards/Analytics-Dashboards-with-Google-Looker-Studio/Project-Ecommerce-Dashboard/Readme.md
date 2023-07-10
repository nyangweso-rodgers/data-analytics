# Project: Ecommerce Dashboard

# Project Description
* We leverage the power of `Google BigQuery` to store a sample Ecommerce data, retrieve the data using a `Connector` and finally perform visualization using the `Google Looker Studio`.

# Data Source Description
* Check out the [My Datasets Repo for a list of datasets](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Datasets) for various analysis. In this project, we use the [online-retail.csv](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Datasets/online-retail.csv) sample data.

# Exploratory Data Analysis with `BigQuery` standard `sql`
* Using `count()` function to get the number of records in the dataset.

    ```sql
        -- count the number of records in the dataset
        select count(*) from FROM `general-364419.table_uploads.online_retail` 
        -- Output: 536,641
    ```

# Data Source Query with standard `sql`
```sql
    with
        online_retail as (
                            SELECT distinct date(InvoiceDate) as InvoiceDate,
                            Country,
                            SAFE_CAST(CustomerID as string) as CustomerID,
                            SAFE_CAST(InvoiceNo as string) as InvoiceNo,
                            SAFE_CAST(StockCode as string) as StockCode,
                            Description,
                            Quantity,
                            UnitPrice,
                            Quantity * UnitPrice as amount
                            FROM `general-364419.table_uploads.online_retail`
                            )

        select *
        from oneline_retail
        where Quantity > 0
        and FORMAT_DATE('%Y%m%d', InvoiceDate) between @DS_START_DATE and @DS_END_DATE
```

# Links
* Looker Studio Data Source Link: https://lookerstudio.google.com/datasources/28765f40-c49f-4980-bacd-c80fbbed07a5
* Looker Studio Dashboard Link: https://lookerstudio.google.com/reporting/86462191-b7d1-4808-ae55-21c8126c5d8d/page/k5SWD
* ![](images/dashboard-output-1.png)