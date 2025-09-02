# Microsoft Power BI

## Table Of Contents

# Setup

- Requirements:
  1.  **Open Database Connectivity** (**ODBC**)
      - **ODBC** is a standard API for accessing **DBMS**. **ODBC** was created to be independent of databases and OS.
      - Download and install **psqlODBC** from the [official website](https://odbc.postgresql.org/).

## 1. ODBC Driver for PostgreSQL

-
- To connect Power BI to a data source such as PostgreSQL, you can use a corresponding ODBC driver.

# PowerBI Architecture

## 1. Storage Mode

- In **Power BI**, there are generally five types of storage modes:

  1. **Import Mode**

     - Oone of the most commonly used **storage modes**. Here, data from the source is imported and stored in Power BI’s internal data model. This means that a copy of the data is taken from the source and loaded into Power BI.
     - **Features**
       1. **Performance**: Since the data is stored locally within Power BI, queries and visualizations are generally very fast. This is because Power BI can access the data directly without needing to query the external source each time
       2. **PowerBI Features**: **Import Mode** supports the full range of Power BI features, including complex calculations, aggregations, and the use of DAX. You can also use all the visualization and reporting capabilities without any restrictions.
       3. **Data Refresh**: Data needs to be refreshed periodically to ensure it is up-to-date. This can be done manually or scheduled to occur automatically at specified intervals.
       4. **Memory Usage**: Since the data is stored within Power BI, it requires sufficient memory to handle the dataset. Large datasets can consume significant memory, which might impact performance if not managed properly.
       5. **Offline Access**: One advantage of Import Mode is that you can work with the data offline, as it is stored within the Power BI file.

  2. **DirectQuery Mode**

     - **Data Storage**: Here, data is not imported into Power BI. Instead, Power BI queries the data directly from the external source each time a report or visualization is accessed.
     - **Features**:

       1. **Real-Time Data**: Since data is queried in real-time, **DirectQuery Mode** is ideal for scenarios where you need the most up-to-date information. This is particularly useful for large datasets or when data changes frequently.
       2. **Performance**: The performance depends on the performance of the underlying data source. If the data source is optimized and can handle large queries efficiently, DirectQuery can perform well. However, it may be slower than Import Mode for complex queries or large datasets.
       3. **Data Transformation**: DirectQuery Mode supports limited data transformations compared to Import Mode. Some complex transformations may not be possible or may need to be handled at the data source level.
       4. **Data Source Compatibility**: **DirectQuery Mode** is compatible with a variety of data sources, including **SQL Server**, **Azure SQL Database**, **Oracle**, and more. However, not all data sources support **DirectQuery**, so it’s important to check compatibility.
       5. **Security**: **DirectQuery Mode** can leverage the security features of the underlying data source, such as row-level security, to ensure that users only see the data they are authorized to access.
       6. **Data Refresh**: Since data is queried in real-time, there is no need for scheduled data refreshes. This can simplify data management and ensure that users always see the most current data.

     - **Considerations and Limitations**:
       1. **Feature Limitations**: There are some limitations on the features available in **DirectQuery Mode**. For example, certain DAX functions and complex calculations may not be supported. Additionally, some Power BI features, like **Quick Insights**, are not available in **DirectQuery Mode**.

  3. **Live Connection**

     - **Live Connection Mode** in is a powerful feature that allows you to connect your reports directly to a published **Power BI semantic model** or an external **Analysis Services model**.
     - **Features**:

       1. **Direct Connection**:

          - **Live Connection Mode** connects your Power BI report directly to a published **semantic model** in the **Power BI service**, **Azure Analysis Services** (**AAS**), or an on-premises **SQL Server Analysis Services** (**SSAS**) instance
          - This means you don’t need to import data into Power BI; instead, you rely on the existing data model.

       2. **Real-Time Data Access**:

          - Since the connection is live, any updates to the data in the **semantic model** are immediately reflected in your Power BI reports.
          - This ensures that your reports always display the most current data without the need for manual or scheduled refreshes.

       3. **Centralized Data Models**

          - **Live Connection Mode** allows multiple reports to use the same centralized semantic model. This promotes consistency and reduces redundancy, as all reports are based on the same data definitions and calculations.
          - It simplifies data management and governance by maintaining a single source of truth.

       4. **Performance**

          - The performance of **Live Connection Mode** depends on the performance of the underlying Analysis Services model. Well-optimized models can provide fast query responses.
          - However, complex queries or large datasets may impact performance, similar to DirectQuery Mode.

       5. **Feature Availability**:

          - Live Connection Mode supports many Power BI features, but some limitations exist. For example, certain data transformations and DAX functions may not be available.
          - You cannot create calculated columns or tables in Live Connection Mode, as the data model is managed externally.

       6. **Security**: **Live Connection Mode** leverages the security features of the underlying Analysis Services model, such as row-level security. This ensures that users only see the data they are authorized to access.

     - **Use Cases**:

       1. **Enterprise Reporting**: Ideal for large organizations that need to maintain a single, centralized data model for consistency across multiple reports and dashboards.
       2. **Real-Time Analytics**: Suitable for scenarios where real-time data access is crucial, and the underlying data model is frequently updated.
       3. **Data Governance**: Helps in maintaining **data governance** by ensuring all reports use the same validated and approved data model.

     - **Considerations and Limitations**
       1. **Dependency on Data Model**: Since the data model is managed externally, any changes to the model can impact all connected reports. Coordination with the data model owner is essential.
       2. **Limited Transformations**: Data transformations need to be handled at the data source level, as Power BI’s transformation capabilities are limited in **Live Connection Mode**.
       3. **Performance**: The performance of your reports depends on the efficiency of the underlying Analysis Services model and the network connection.

  4. **Dual Mode**

     - **Dual Mode** in Power BI is a versatile storage mode that allows a table to act as both **Import** and **DirectQuery**, depending on the context in which the table is used.
     - **Features**:

       1. **Flexible Storage**: Enables a table to switch between **Import** and **DirectQuery modes** based on the query context. This means that the same table can use cached data (Import) for some queries and query the data source directly (DirectQuery) for others.
       2. **Performance Optimization**:
          - By leveraging both **Import** and **DirectQuery**, **Dual Mode** can optimize performance. For example, frequently accessed data can be cached for faster performance, while less frequently accessed data can be queried in real-time.
          - This helps balance the load on the data source and improves the overall responsiveness of reports.
       3. **Aggregation Support**: **Dual Mode** is particularly useful for scenarios involving **aggregations**. Aggregated tables can be stored in **Import Mode** for quick access, while detailed data can be queried in **DirectQuery Mode** as needed.
       4. **Reduced Data Latency**: **Dual Mode** can help reduce data latency by caching only the necessary data. This ensures that users get near-real-time data without the need to refresh the entire dataset.
       5. **Simplified Data Management**: With **Dual Mode**, you can manage large datasets more efficiently by caching only the most critical data. This reduces memory usage and improves data refresh times.

     - **Use Cases**:

       1. Large Datasets: Ideal for large datasets where importing all data is impractical. Dual Mode allows you to cache critical data while querying the rest in real-time.
       2. Performance Optimization: Useful for optimizing performance by caching frequently accessed data and querying less frequently accessed data directly.
       3. Aggregations: Beneficial for scenarios involving aggregations, where aggregated data can be cached for quick access, and detailed data can be queried as needed.

     - **Considerations and Limitations**
       1. Complexity: Managing Dual Mode can be more complex than using a single storage mode. It requires careful planning to determine which data should be cached and which should be queried in real-time.
       2. Data Source Performance: The performance of DirectQuery queries in Dual Mode still depends on the efficiency of the underlying data source

  5. **Push Mode**

     - **Push Mode** is designed for real-time data streaming and updates.
     - **Features**:

       1. **Real-Time Data Streaming**

          - Push Mode allows you to stream data into Power BI in real-time. This is achieved by pushing data to a Power BI dataset using the Power BI REST API.
          - This mode is ideal for scenarios where you need to display live data, such as monitoring dashboards or real-time analytics.

       2. **Push Datasets**

          - A **push dataset** is a special type of dataset in Power BI that can receive data pushed from an external source. These datasets are created and managed through the Power BI REST API.
          - Push datasets can be used to create real-time dashboards and reports that update automatically as new data is received.

       3. **Data Storage**: Data pushed to a **push dataset** is stored in Power BI and can be used for historical analysis. This allows you to combine real-time data with historical data for comprehensive insights.

       4. **Performance**

          - **Push Mode** can provide better performance and scalability compared to **DirectQuery**, as it avoids the need to query the data source for every report update.
          - However, the performance depends on the volume of data being pushed and the frequency of updates.

       5. **API Integration**
          - **Push Mode** relies on the **Power BI REST API** to send data to the dataset. This requires some development effort to set up the API calls and manage the data flow.
          - You can use various tools and programming languages to interact with the Power BI REST API, such as Python, .NET, or PowerShell.

     - **Use Cases**

       1. **Real-Time Dashboards**: Ideal for creating dashboards that need to display live data, such as monitoring systems, IoT dashboards, or live event tracking.
       2. **Operational Reporting**: Useful for scenarios where operational data needs to be updated frequently and displayed in near real-time.
       3. **Hybrid Models**: Can be combined with **Import Mode** to create hybrid models that leverage both historical and real-time data.

     - **Considerations and Limitations**
       1. Development Effort: Setting up push datasets and integrating with the Power BI REST API requires some development effort and technical knowledge.
       2. Data Volume: While push datasets can handle real-time data, there are limits to the volume of data that can be pushed and stored. It’s important to manage the data flow to avoid performance issues.
       3. Feature Limitations: Some Power BI features may not be fully supported with push datasets. It’s important to test and validate your reports to ensure they meet your requirements.

# PowerBI Features

## 1. Calculated Columns

- **Calculated columns** in Power BI are similar to columns in a spreadsheet—they are calculated row-by-row and stored as part of the underlying data model. **Calculated columns** are computed during data refresh and become a permanent part of the dataset. They are typically used to create new columns based on existing data in the dataset.
- How to Create **Calculated Columns**?:

  1. Select the table in which you want to create the new column.
  2. Select the ‘New Column’ option from the top.

- When to Use **Calculated Columns**?:

  1. **Static Data Manipulation**: Calculated columns are useful for tasks such as concatenating strings, performing mathematical operations, or deriving new categorical variables based on existing columns.
  2. **Filtering and Sorting**: Calculated columns can facilitate filtering and sorting operations that depend on computed values.
  3. **Relationships and Joins**: Calculated columns can aid in establishing relationships between tables by creating common fields for joining.

- Considerations for **Calculated Columns**:
  1. **Data Refresh Performance**: Adding calculated columns can increase data refresh times, especially for large datasets. Consider the performance implications when adding numerous calculated columns.
  2. **Storage and Memory**: Calculated columns consume storage space within the data model, so avoid creating unnecessary calculated columns to conserve resources.
  3. **Limited Aggregation**: Calculated columns cannot be aggregated across rows in visualizations; they are only available at the row level.

## 2. Measures

- **Measures** in **Power BI** are dynamic calculations that are computed at query time based on the context of the visualization or calculation. Unlike **calculated columns**, **measures** are not stored in the **data model**; instead, they are calculated on the fly in response to user interactions or data queries.
- **How to Create Calculated Measure**?

  1. Select the ‘New Measure’ option from the top on the Home page.

- **When to Use Measures?**:

  1. **Aggregations and Calculations**: Measures are ideal for performing aggregations (sums, averages, counts) and complex calculations (such as ratios, percentages, and averages) across multiple rows or tables.
  2. **Dynamic Context**: **Measures** adjust their calculations based on the filters, slicers, or context applied within a visualization, providing dynamic insights into the data.
  3. **Reusability**: Measures can be reused across multiple visualizations and reports, promoting consistency and efficiency in report development.

- **Considerations for Measures**:
  1. **Performance Optimization**: Well-written measures can improve report performance by offloading calculations to the query engine and reducing the data transferred between Power BI and the data source.
  2. **Context Sensitivity**: Understand how measures respond to changes in context, such as slicers, filters, or drill-down actions, to ensure accurate results in different scenarios.
  3. **DAX Language Proficiency**: Developing complex measures may require proficiency in the **Data Analysis Expressions** (**DAX**) language, which powers calculations in Power BI.

## 3. Data Analysis Expression (DAX) Queries

- Example of DAX Queries:
  1. `Revenue = SUM('Sales'[Total])*5/234`

## 4. Power BI Incremental Refresh

- **Incremental Refresh** is the process of loading **changed** or **new data** from a transactional database into the data warehouse.
- **Advantages of Power BI Incremental Refresh**:

  1. **Lower Refresh Times** – In order to refresh the data model from the entire database, it might take a long time. However, in the case of an incremental refresh, since we are bringing in only a small portion of the data, refreshes are generally faster
  2. **More Reliable Queries** – As we refresh a Power BI data model, in the background SQL queries are being fired to the database engine. Queries that run for a shorter duration are more reliable as they do not lock the database for long periods
  3. **Less Resource Consumption** – Due to the lower runtime of the queries, the database resources consumed are also quite less

- Pre-Requisites while setting up Power BI Incremental Refresh:

  1. **Import Data Mode** – The data should be imported into the Power BI data model in **Import Data mode**. Incremental refresh doesn’t work with the **Direct Query mode**
  2. **Power BI Data Gateway** – In order to access the on-premises data sources by the **Power BI Service**, we need to set up the Data Gateway. This is not mandatory if your source data is present in the cloud

- **Setting up the Power BI Incremental Refresh in Power BI Desktop**:
  1. Creating the Report
  2. Setting up the range parameters
     - In order to set up the **Power BI Incremental Refresh**, we need to dynamically modify the database query that Power BI will generate. This is done by using **parameters** in **Power BI**. The **parameters** will dynamically generate the date values based on the data available in the Power BI data model and will generate the query that would fetch records after the last present data in the model.
     - For this, we need to create two **parameters** using the reserved keywords – **RangeStart** and **RangeEnd**. Click on **Transform** and then select **Manage Parameters**. It is mandatory that the datatype for both these parameters must be of `Date/Time`. A default datetime value can be added to the parameter that will be used to create the initial query.
     - Once the parameters are set up correctly, the next step is to filter the Power Query. Click on the button on the column that you want the filters to be added. In this case, we would want the data to be filtered based on the OrderDate values. Select **Date/Time Filters** and then **Custom Filter**.
  3. Defining the Power BI Incremental Refresh policy
     - This can be done by navigating to the Fields pane on the Power BI Desktop and then right-click on the table on which the policy is to be applied.

# Charts

## 1. Waterfall Charts

- Best for **visualizing step-by-step changes** in numbers, especially when considering increases/decreases per phase.
- Helps answer: “Out of X sales, how many progressed, how many dropped off?
- Works well for financial and transactional flows

- **Steps to implement Waterfall Chart in PowerBI**

  1. **Step 1**: Prepare Data for the Waterfall Chart
     - We want to calculate the number of customers at each stage per month to visualize their transition.
     - Create Measures for Each Step: We will define the measures in DAX based on `sale_month`, `dispatch_month`, and `refund_month`
       1. **Total Sales**: This counts the number of customers who made a sale in a given month.
          ```DAX
            // DAX
            TotalSales =
              CALCULATE(
                  DISTINCTCOUNT( accounts_mashup_cte[customer_id] ),
                  accounts_mashup_cte[sale_month]
              )
          ```
       2. **Total Dispatches**: This counts the number of customers whose kits were dispatched in a given month.
          ```DAX
            TotalDispatches =
              CALCULATE(
                  DISTINCTCOUNT( accounts_mashup_cte[customer_id] ),
                  accounts_mashup_cte[dispatch_month]
              )
          ```
       3. **Total Refunds**: This counts the number of customers who requested refunds.
          ```DAX
            TotalRefunds =
              CALCULATE(
                  DISTINCTCOUNT( accounts_mashup_cte[customer_id] ),
                  accounts_mashup_cte[refund_month]
              )
          ```
       4. **Net Sales After Refunds**: To show the remaining after refunds:
          ```DAX
            NetSalesAfterRefunds = [TotalSales] - [TotalRefunds]
          ```
       5. **Remaining After Dispatch**: To show how many proceeded beyond dispatch:
          ```DAX
            RemainingAfterDispatch = [NetSalesAfterRefunds] - [TotalDispatches]
          ```

- To create Waterfall, need to specify:
  1. Category
  2. Breakdown
  3. Values

## 2. Funnel Chart

- Ideal for **depicting drop-offs** at various stages
- Useful when emphasizing the number of clients who remain versus those lost at each phase.
- Helps answer: “Where are the biggest customer losses happening?”

## 3. Gannt Chart

- Typically used for **tracking timelines** and project progress.
- Not the best fit for client transitions unless you're focusing on time-based trends of customer journeys.

# Copilot For PowerBI

- **Examples of custom requests**:

  1. What are some key sales insights on this page?
  2. What are some interesting customer segments?
  3. What is the relationship between product type and revenue?

- **Considerations**
  - **Copilot** in Microsoft Fabric isn’t supported on trial SKUs. Only paid SKUs (F64 or higher, or P1 or higher) are supported.

# Resources and Further Reading

1. [Calculated Columns vs. Measures in Power BI](https://www.c-sharpcorner.com/article/calculated-columns-vs-measures-in-power-bi/?ref=dailydev)
2. [SQLShack - An overview of Power BI Incremental Refresh](https://www.sqlshack.com/an-overview-of-power-bi-incremental-refresh/?ref=dailydev)
3. [Medium - Unlocking the Power of Power BI: Mastering Storage Mode](https://medium.com/microsoft-power-bi/unlocking-the-power-of-power-bi-mastering-storage-mode-e5091c149c1a)
