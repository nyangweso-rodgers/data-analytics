# Python Analytics For E-Commerce Data

## Table Of Contents

# Analysis

1. [Expploratory Data Analysis]()
2. [Monthly Customer Retention Analysis]()
3. [Weekly Customer Retention Analysis]()
4. [Customer Segmentation Analysis]()
5. [Customer Lifetime Value Analysis]()

# 2. Retention Analytics

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

1. [https://www.mth548.org/Tools/Seaborn/seaborn_plot_types.html](https://www.mth548.org/Tools/Seaborn/seaborn_plot_types.html)
2. [Predicting and Preventing the Churn of High Value Customers Using Machine Learning](https://towardsdatascience.com/predicting-and-preventing-the-churn-of-high-value-customers-using-machine-learning-adbb4a61095d)
3. [Towards Data Science - Divide and Conquer: segment your customers using RFM analysis](https://towardsdatascience.com/divide-and-conquer-segment-your-customers-using-rfm-analysis-68aee749adf6)
