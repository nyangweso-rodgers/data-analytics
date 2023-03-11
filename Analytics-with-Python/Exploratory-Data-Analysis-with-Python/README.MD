# Exploratory Data Analysis (EDA) with Python

## Table Of Contents
- [Exploratory Data Analysis (EDA) with Python](#exploratory-data-analysis-eda-with-python)
  - [Table Of Contents](#table-of-contents)
- [Description](#description)
- [Why Exploratory data analysis (EDA) is an essential task](#why-exploratory-data-analysis-eda-is-an-essential-task)
- [Exploratory data analysis (EDA) steps.](#exploratory-data-analysis-eda-steps)
- [Descriptive statistics.](#descriptive-statistics)
- [How to check for Duplicates](#how-to-check-for-duplicates)
- [DATA CLEANING](#data-cleaning)
# Description
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

# How to check for Duplicates
* The __pandas.DataFrame.duplicated.sum()__ function returns a Series or DataFrame containing the sum of the boolean values (True or False) in the duplicate rows of the DataFrame.

# DATA CLEANING
* __Data cleaning__ is the process of fixing or removing incorrect, corrupted, incorrectly formatted, duplicate, or incomplete data within a dataset. It involves
  * handling missing values and 
  * handling outliers