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

## Exploratory Data Analysis Steps

### Step 1: Understamd Stakeholders Objectives

- Develop a solid understanding of the decisions that the Stakeholders need to make, or the types of changes/ interventions they need to make calls on.

### Step 2: Summarize analysis goals and get alignment

- These conversations will help you determine the analysis goals, i.e., whether you should focus on identifying patterns and relationships, understanding distributions, etc. Summarize your understanding of the goal(s), specify an analysis period and population, and make sure all relevant stakeholders are aligned. At this point, I also like to communicate non-goals of the analysis — things stakeholders should not expect to see as part of my deliverable(s).

### Step 3: Develop a list of research questions

- Create a series of questions related to the analysis goals you would like to answer, and note the **dimensions** you’re interested in exploring within, i.e., specific time periods, new users, users in a certain age bracket or geographical area, etc.

### Step 4: Identify your knowns and unknowns

- Collect any previous research, organizational lore, and widely accepted assumptions related to the analysis topic. Review what’s been previously researched or analyzed to understand what is already known in this arena.
- Make note of whether there are historical answers to any of your analysis questions. Note: when you’re determining how relevant those answers are, consider the amount of time since any previous analysis, and whether there have been significant changes in the analysis population or product/ service since then.

### Step 5: Understand what is possible with the data you have

- Once you’ve synthesized your goals and key questions, you can identify what relevant data is easily available, and what supplemental data is potentially accessible. Verify your permissions to each data source, and request access from data/ process owners for any supplemental datasets. Spend some time familiarizing yourself with the datasets, and rule out any questions on your list it’s not possible to answer with the data you have.

### Step 6: Set expectations for what constitutes one analysis

- Do a prioritization exercise with the key stakeholder(s), for example, a product manager, to understand which questions they believe are most important. It’s a good idea to T-shirt size (S, M, L) the complexity of the questions on your list before this conversation to illustrate the level of effort to answer them. If the questions on your list are more work than is feasible in a single analysis, use those prioritizations to determine how to stagger them into multiple analyses.

### Step 7: Transform and clean the data as necessary

- If data pipelines are in place and data is already in the format you want, evaluate the need for data cleaning (looking for outliers, missingness/ sparse data, duplicates, etc.), and perform any necessary cleaning steps. If not, create data pipelines to handle any required relocation or transformations before data cleaning.

### Step 8: Use summary statistics to understand the “shape” of data

- Start the analysis with high-level statistical exploration to understand distributions of features and correlations between them. You may notice data sparsity or quality issues that impact your ability to answer questions from your analysis planning exercise. It’s important to communicate early to stakeholders about questions you cannot address, or that will have “noisy” answers less valuable for decision-making.

### Step 9: Answer your analysis questions

- At this stage, you’ll move into answering the specific questions you developed for the analysis. I like to visualize as I go, as this can make it easier to spot patterns, trends, and anomalies, and I can drop interesting visuals right into my write-up draft.
- Depending on the type of analysis, you may want to generate some additional features (ex: bucket ranges for a numeric feature, indicators for whether a specific action was taken within a given period or more times than a given threshold) to explore correlations further, and look for less intuitive relationships between features using machine learning.

### Step 10: Document your findings

- As you conduct your analysis, note answers you find under each question. Highlight findings you think are interesting, and make notes on any trains of thought a finding sparked.

### Step 11: Share your findings

- When your analysis is ready to share with the original stakeholders, be thoughtful about the format you choose. Depending on the audience, they may respond best to a Slack post, a presentation, a walkthrough of the analysis document, or some combination of the above

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
5. [towardsdatascience.com - exploratory-data-analysis-in-11-steps-31a36ae0b407](https://towardsdatascience.com/exploratory-data-analysis-in-11-steps-31a36ae0b407)
