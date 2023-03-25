# Customer Segmentation Analysis with Python - RFM

## Table Of Contents
- [Customer Segmentation Analysis with Python - RFM](#customer-segmentation-analysis-with-python---rfm)
  - [Table Of Contents](#table-of-contents)
- [Theory: RFM Analysis](#theory-rfm-analysis)
- [RFM Use Cases](#rfm-use-cases)
- [Possible Consideraion Segmentation Path](#possible-consideraion-segmentation-path)
- [Quartiles](#quartiles)

# Theory: RFM Analysis
* RFM Analysis is based on:
  * __Recency__: number of days that have passed since the customer last purchased - _How recently did the customer purchase_?
  * __Frequency__: number of purchases in a specific period (for example, last 12 months) - _How often do they purchase_?.
  * __Monetary__: Value of Order in the specific period – _How much do they spend_?.

* It is based on __Pareto principle__: Targeting 20% of customers generate 80% of revenue.

# RFM Use Cases
* Churn Analysis
* Customer Cluster Analysis
* Marketing Analytics (Mailing ,Campaign)

# Possible Consideraion Segmentation Path
* We can consider with the following marketing segmenting path to boost the sales:

    Segment | Score | Description | Marketing Decision
    |-------|------------|--------|----------|
    Best / Top Customer | 111 | Bought most recently, most often and spend the most | Launch loyalty programme for this segment
    Loyal Customer | x1x | Buy most often | Use Recency and Monetary value for further analysis
    Cash Rich / Big Spender | xx1 | Spend highest | offer most expensive product
    Lost Customer | 311 and 411 | Haven't purchased for some time, but purchased frequently and spend the most | Price discount
    Cheap/Death Customer | 444 | Customer is not active at all to long ago, purchase few and spend low | Don't spend to much except Reacquire

# Quartiles
* __Quartiles__ are values that separate the data into _four_ equal parts.
    -  __Q0__ is the smallest value in the data.
    - __Q1__ is the value separating the first quarter from the second quarter of the data.
    - __Q2__ is the middle value (median), separating the bottom from the top half.
    - __Q3__ is the value separating the third quarter from the fourth quarter
    - __Q4__ is the largest value in the data.

