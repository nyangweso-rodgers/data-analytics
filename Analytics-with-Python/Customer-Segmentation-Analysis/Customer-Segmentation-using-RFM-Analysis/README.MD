# Example 1: Oneline Retail Store Data

## Table Of Contents
- [Further Reading]()
  - [Towards Data Science - Divide and Conquer: segment your customers using RFM analysis](https://towardsdatascience.com/divide-and-conquer-segment-your-customers-using-rfm-analysis-68aee749adf6)

# Dataset Description
* In this __RFM Analysis__, we use the [grouped daily customer data](https://raw.githubusercontent.com/nyangweso-rodgers/Data_Analytics/main/Analytics-with-Python/Exploratory-Data-Analysis-with-Python/Exploratory-Data-Analysis-for-Online-Retail-Store/grouped_daily_customer_data.csv) generated from the [Exploratory Data Analysis for Online Retail Store](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics-with-Python/Exploratory-Data-Analysis-with-Python/Exploratory-Data-Analysis-for-Online-Retail-Store)

# RFM Analssis - Theory
* __RFM Analysis__ is a customer segmentation method based on:
  1. __Recency__: number of days that have passed since the customer last purchased - How recently did the customer purchase?
  2. __Frequency__: number of purchases in a specific period (for example, last 12 months) - How often do they purchase.
  3. __Monetary__: Value of Order in the specific period – How much do they spend.

* __It is based on Pareto principle__: _Targeting 20% of customers generate 80% of revenue_.

# RFM Use Cases
* Churn Analysis
* Customer Cluster Analysis
* Marketing Analytics (Mailing ,Campaign)
# RFM Questions of Interests
* __RFM segmentation__ readily answers these questions for your business…:
  1. Who are my best customers?
  2. Which customers are at the verge of churning?
  3. Who are lost customers that you don’t need to pay much attention to?
  4. Who are your loyal customers?
  5. Which customers you must retain?
  6. Who has the potential to be converted into more profitable customers?
  7. Which group of customers is most likely to respond to your current campaign?

# Possible Consideraion Segmentation Path
* We can consider with the following marketing segmenting path to boost the sales:

    Segment | Score | Description | Marketing Decision
    |-------|------------|--------|----------|
    Best / Top Customer | 111 | Bought most recently, most often and spend the most | Launch loyalty programme for this segment
    Loyal Customer | x1x | Buy most often | Use Recency and Monetary value for further analysis
    Cash Rich / Big Spender | xx1 | Spend highest | offer most expensive product
    Lost Customer | 311 and 411 | Haven't purchased for some time, but purchased frequently and spend the most | Price discount
    Cheap/Death Customer | 444 | Customer is not active at all to long ago, purchase few and spend low | Don't spend to much except Reacquire


# Stpes for RFM Analysis

# Defining Quartiles
* __Quartiles__ are values that separate the data into _four_ equal parts.
    - __Q0__ is the smallest value in the data.
    - __Q1__ is the value separating the first quarter from the second quarter of the data.
    - __Q2__ is the middle value (median), separating the bottom from the top half.
    - __Q3__ is the value separating the third quarter from the fourth quarter
    - __Q4__ is the largest value in the data.