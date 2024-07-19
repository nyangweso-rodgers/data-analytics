# How to Handle Missing Data in Python

## Table Of Contents
- [Further Reading]() 
  - [How to handle missing Values in a dataset using Python.](https://dev.to/phylis/how-to-handle-missing-values-in-a-dataset-using-python-1d2e)

# Data Source
* In this Project, we use the [Online Retail Store](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Datasets/Online_Retail.csv) data from [Index of /ml/machine-learning-databases/00352](http://archive.ics.uci.edu/ml/machine-learning-databases/00352/)

# How to Check for Missing Values
* We check for missing values by utilizing the __isna()__ method, which returns a dataframe of boolean values indicating whether or not a field is null. We can use the __sum()__ method to group all missing values by column.

# Deleting the columns/rows with missing data
* From [pandas.DataFrame.dropna - official documentation](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.dropna.html), __dropna()__ function is used to remove rows and columns with __Null/NaN__ values. Also if there is a certain row with missing data, then you can delete the entire row with all the features in that row.
  * __axis=1__ is used to drop the column with __NaN__ values.
  * __axis=0__ is used to drop the row with __NaN__ values.

# Filling the missing data with a value- Imputation

# Imputation with an additional column