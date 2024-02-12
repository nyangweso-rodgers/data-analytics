# How To Filter Pandas DataFrame

## Table Of Contents
- [Further Reading]() 
  - [Different methods to filter a Pandas DataFrame](https://dev.to/aviatorifeanyi/different-methods-to-filter-a-pandas-dataframe-48of)


# Description
* Data Filtering is one of the data manipulation operations we can perform when working with data. Filtering with pandas is similar to a WHERE clause in SQL or a filter in Microsoft Excel.
  
* In pandas, there is more than one way to operate. Pandas provide multiple ways to filter data.
  * __isin()__: This method provides a way to apply single or multiple conditions for filtering.
  * __Logical operators__: We can use the available logical operators to filter for a subset of data
  * __Query Function__: The query function takes in an expression as an argument which evaluates to a Boolean that is used to filter the dataframe.
  * __Str Accessor__: Pandas make it easy to work with string values. Using the str accessor, we can filter for records whose values are strings.
  * __nlargest and nsmallest__: Most times, we just need records of the highest or lowest values in a column. These methods make it possible. 
  * __Tilde sign (~)__: Used to reverse the logic used in filter condition
  * __isnull | notnull__: Using the isnull method, we can return records that have NaN values and mark them for deletion. Using the notnull method, we can filter for records
  * that do not contain NaN values.
  * __Filter__: Using this method, we can also filter for a subset of data.

# Data Source
* For this project, we the [online_retail_data_without_null_values.csv](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Analytics-with-Python/Python-Modules-for-Data-Analysis/Pandas-Module/How-to-Handle-Missing-Data-in-Pandas-DataFrame/online_retail_data_without_null_values.csv) dataset from the original, [Online Retail Store Data](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Datasets/Online_Retail.csv)
  
# Examples Of Filtering
1. Get Transactions from Brazil
2. Get transactions greater than $20000
3. e.t.c.,