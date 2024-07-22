# Python Analytics For E-Commerce Data

## Table Of Contents

# Analysis

1. [Expploratory Data Analysis]()
2. [Monthly Customer Retention Analysis]()
3. [Weekly Customer Retention Analysis]()
4. [Customer Segmentation Analysis]()
5. [Customer Lifetime Value Analysis]()

# 1. Exploratory Data Analysis (EDA)

- **Exploratory data analysis** (EDA) is an essential step in the data science process which involves use of both _statistical methods_ and _data visualization techniques_ to:
  - uncover patterns, trends,
  - understand relationships and
  - gain meaningful insights from data in order to understand the problem and make informed decisions.

## Why Exploratory data analysis (EDA) is an essential task

- EDA helps data practitioners understand and gain insights from data before applying machine learning and statistical techniques.
- EDA helps identify patterns, anomalies, and relationships within the data so as to make informed decisions and develop effective strategies.
- The EDA process aims in detecting faulty points in data such as errors or missing values which can be corrected by analysis.

## Data Cleaning

- **Data cleaning** is the process of fixing or removing incorrect, corrupted, incorrectly formatted, duplicate, or incomplete data within a dataset. It involves
  - handling missing values and
  - handling outliers

## Univariate Analysis.

- **Univariate analysis** is a form of exploratory data analysis (EDA) that involves the examination of a single variable. It is used to summarize the data and gain insight into the data's distribution, central tendency, and variability. It can be used to answer questions such as what is the range of the data, what is the most common value, and is there any outliers. It is also used to identify any trends or patterns in the data.

## Bivariate Analysis

- **Bivariate analysis** involves analyzing data with two variables or columns. This is usually a way to explore the relationships between these variables and how they influence each other, if at all.

## Multivariate Analysis

- Involves examining more than two variables at once in order to better understand relationships between them.

## Charts

1. Line Charts
2. Bar Charts
3. Pie Charts
4. Histogram
5. Scatter Plots
6. Box Plot
7. Violing Plot
8. Heatmap
9. Area Chart
10. Radar Chart

# 2. Retention Analytics

- **Churn analysis** is the process of studying customer behavior to identify factors that contribute to a customer's decision to stop doing business with a company.

## Build a churn prediction model

- This may involve using techniques such as **logistic regression** or **decision trees** to predict whether or not a customer is likely to churn based on their behavior.

# 4. Customer Segmentation Analsis

## RFM Analssis - Theory

- **RFM Analysis** is a **customer segmentation** method based on:
  1. **Recency**: number of days that have passed since the customer last purchased - How recently did the customer purchase?
  2. **Frequency**: number of purchases in a specific period (for example, last 12 months) - How often do they purchase.
  3. **Monetary**: Value of Order in the specific period – How much do they spend.

* **It is based on Pareto principle**: _Targeting 20% of customers generate 80% of revenue_.

## RFM Use Cases

- Churn Analysis
- Customer Cluster Analysis
- Marketing Analytics (Mailing ,Campaign)

## RFM Questions of Interests

- **RFM segmentation** readily answers these questions for your business…:
  1. Who are my best customers?
  2. Which customers are at the verge of churning?
  3. Who are lost customers that you don’t need to pay much attention to?
  4. Who are your loyal customers?
  5. Which customers you must retain?
  6. Who has the potential to be converted into more profitable customers?
  7. Which group of customers is most likely to respond to your current campaign?

## Possible Consideraion Segmentation Path

- We can consider with the following marketing segmenting path to boost the sales:

  | Segment                 | Score       | Description                                                                  | Marketing Decision                                  |
  | ----------------------- | ----------- | ---------------------------------------------------------------------------- | --------------------------------------------------- |
  | Best / Top Customer     | 111         | Bought most recently, most often and spend the most                          | Launch loyalty programme for this segment           |
  | Loyal Customer          | x1x         | Buy most often                                                               | Use Recency and Monetary value for further analysis |
  | Cash Rich / Big Spender | xx1         | Spend highest                                                                | offer most expensive product                        |
  | Lost Customer           | 311 and 411 | Haven't purchased for some time, but purchased frequently and spend the most | Price discount                                      |
  | Cheap/Death Customer    | 444         | Customer is not active at all to long ago, purchase few and spend low        | Don't spend to much except Reacquire                |

## Steps for RFM Analysis

### Defining Quartiles

- **Quartiles** are values that separate the data into _four_ equal parts.
  - **Q0** is the smallest value in the data.
  - **Q1** is the value separating the first quarter from the second quarter of the data.
  - **Q2** is the middle value (median), separating the bottom from the top half.
  - **Q3** is the value separating the third quarter from the fourth quarter
  - **Q4** is the largest value in the data.

## Segmentation with K-Means

### Overview Of K-Means Clustering

- The **K-Means** model works by finding different groups (clusters) of data points that share similar properties within the dataset. It does that by grouping the points in such a way that minimizes the distance of all the points to the centroid of the cluster in which they are contained.

- In other words: the main objective of the algorithm is to find clusters so that the distance between data points in the same cluster is smaller than the distance between any two points in different clusters. This way, members that fall on the same group tend to share similar characteristics and be different from members of other groups.

# 5. Customer Lifetime Value (CLV) Analysis

- For the **CLV** models, the following nomenclature is used:
  - **Frequency** represents the number of repeat purchases the customer has made. This means that it’s one less than the total number of purchases.
  - `T` represents the **age** of the customer in whatever time units chosen (daily, in our dataset). This is equal to the duration between a customer’s first purchase and the end of the period under study.
  - **Recency** represents the age of the customer when they made their most recent purchases. This is equal to the duration between a customer’s first purchase and their latest purchase. (Thus if they have made only 1 purchase, the recency is 0.)

# Resources and Further Reading

1. [Dev Community - How to Perform Exploratory Data Analysis with Python](https://dev.to/phylis/how-to-perform-exploratory-data-analysis-with-python-l8j)
2. [https://www.mth548.org/Tools/Seaborn/seaborn_plot_types.html](https://www.mth548.org/Tools/Seaborn/seaborn_plot_types.html)
3. [Predicting and Preventing the Churn of High Value Customers Using Machine Learning](https://towardsdatascience.com/predicting-and-preventing-the-churn-of-high-value-customers-using-machine-learning-adbb4a61095d)
4. [Towards Data Science - Divide and Conquer: segment your customers using RFM analysis](https://towardsdatascience.com/divide-and-conquer-segment-your-customers-using-rfm-analysis-68aee749adf6)
