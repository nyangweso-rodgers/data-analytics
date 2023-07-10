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

# Links
* Looker Studio Data Source Link: https://lookerstudio.google.com/datasources/28765f40-c49f-4980-bacd-c80fbbed07a5
* Looker Studio Dashboard Link: https://lookerstudio.google.com/reporting/86462191-b7d1-4808-ae55-21c8126c5d8d/page/k5SWD
* ![](images/dashboard-output-1.png)