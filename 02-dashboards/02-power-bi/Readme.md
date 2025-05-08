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

## 3. Power BI Incremental Refresh

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

## Waterfall Charts

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

## Funnel Chart

- Ideal for **depicting drop-offs** at various stages
- Useful when emphasizing the number of clients who remain versus those lost at each phase.
- Helps answer: “Where are the biggest customer losses happening?”

## Gannt Chart

- Typically used for **tracking timelines** and project progress.
- Not the best fit for client transitions unless you're focusing on time-based trends of customer journeys.

# Data Analysis Expressions (DAX)

# Resources and Further Reading

1. [Calculated Columns vs. Measures in Power BI](https://www.c-sharpcorner.com/article/calculated-columns-vs-measures-in-power-bi/?ref=dailydev)
2. [SQLShack - An overview of Power BI Incremental Refresh](https://www.sqlshack.com/an-overview-of-power-bi-incremental-refresh/?ref=dailydev)
