# Exploratory Data Analysis (EDA) with Python

## Table Of Contents
-  [Data Analysis Projects]()
   -  [Data Analysis for an Online Retail Store Data]()
      -  [Exploratory Data Analysis]()
      -  [Data Visualization]()
      -  [Customer Retention Analysis]()
      -  [Customer Segmentation Analysis]()

- [Further Reading]() 
  - [Dev Community - How to Perform Exploratory Data Analysis with Python](https://dev.to/phylis/how-to-perform-exploratory-data-analysis-with-python-l8j)
# Description
* This folder contains various data analysis projects based on different datasets. The major components of the analyses are:
    1. Exploratory Data Analysis
    2. Data Visualizations
    3. Customer Analytics
       1. Retention Analytics
       2. Segmentation Analytics


# Exploratory Data Analysis (EDA)
* __Exploratory data analysis__ (EDA) is an essential step in the data science process which involves use of both _statistical methods_ and _data visualization techniques_ to:
  * uncover patterns, trends, 
  * understand relationships and 
  * gain meaningful insights from data in order to understand the problem and make informed decisions. 

# Why Exploratory data analysis (EDA) is an essential task
* EDA helps data practitioners understand and gain insights from data before applying machine learning and statistical techniques.
* EDA helps identify patterns, anomalies, and relationships within the data so as to make informed decisions and develop effective strategies.
* The EDA process aims in detecting faulty points in data such as errors or missing values which can be corrected by analysis.

# Exploratory data analysis (EDA) steps.
1. Import necessary Libraries
    ```py
        import pandas as pd
        import numpy as np
        import seaborn as sns
        import matplotlib.pyplot as plt
    ```
2. Load the dataset
3. Viewing the dataset.
4. Check for duplication.
5. Data preparation (Handling missing values and outliers)
6. Analyzing the data.(univariate, bivariate and multivariate analysis)
7. Visualization

# Descriptive statistics.
* The __describe()__ function in pandas is used to generate descriptive statistics that summarize the central tendency, dispersion and shape of a dataset's distribution, excluding NaN values. It provides the count, mean, standard deviation, minimum, maximum, 25th percentile, 50th percentile (median) and 75th percentile of the data.



# DATA CLEANING
* __Data cleaning__ is the process of fixing or removing incorrect, corrupted, incorrectly formatted, duplicate, or incomplete data within a dataset. It involves
  * handling missing values and 
  * handling outliers

# Univariate Analysis.
* __Univariate analysis__ is a form of exploratory data analysis (EDA) that involves the examination of a single variable. It is used to summarize the data and gain insight into the data's distribution, central tendency, and variability. It can be used to answer questions such as what is the range of the data, what is the most common value, and is there any outliers. It is also used to identify any trends or patterns in the data.
# Bivariate Analysis
* Bivariate analysis involves analyzing data with two variables or columns. This is usually a way to explore the relationships between these variables and how they influence each other, if at all.
# Multivariate Analysis
* Involves examining more than two variables at once in order to better understand relationships between them.