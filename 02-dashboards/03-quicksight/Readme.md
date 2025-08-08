# AWS QuickSight

## Table Of Contents

# AWS QuickSight

- **Amazon QuickSight** is AWS’s AI-powered business intelligence (BI) service that helps customers get insights faster and make better decisions.
- **Benefits**:

  1.  Best for AWS-native environments
  2.  Scales to thousands of users at low cost (especially with the "Reader" pricing model)
  3.  Embedding and API-first — great for integrating into your products or portals
  4.  Supports machine learning insights natively (anomaly detection, forecasting, etc.)

- **Workflow in QuickSight**:
  1.  Create an Analysis — build visuals, filters, controls
  2.  Test with SPICE or live data
  3.  Add filters + controls for readers
  4.  Publish as a Dashboard
  5.  Share the Dashboard with users or groups

# Featueres

## Session

- A **30-minute** window during which a user (**Reader**) accesses one or more dashboards, regardless of how many dashboards they view in that time.

## Super-fast, Parallel, In-memory Calculation Engine(SPICE)

- SPICE is QuickSight’s high-performance in-memory data store that:
  - Ingests and stores data within QuickSight (instead of querying your data source every time)
  - Accelerates queries and dashboards with fast in-memory computation
  - Handles large datasets with parallel processing
- **Benefits**:

  1. In-memory engine: Faster performance than direct queries
  2. Schedule refreshes: Keeps data up-to-date without constant reloading
  3. High concurrency support: SUpports many users querying the dashboard at once
  4. Cost control: Reduce load on the source systems (less quertying = cost savings)
  5. Scale easily: Can handle millions of rows with good performance

- SPICE vs. Direct Query
  - SPICE: for speed, offline access, large read-heavy dashboards
  - Direct Query: When you need real-time data or want to avoid storing a compy in QuickSight

## Analysis

- **Features**:
  1. Exploration:Used by Authors/Analysts to explore data and build insights
  2. Workspace: Temporary and personal — not shared with end users yet
  3. Auto-refresh: Can run live queries or SPICE, updates happen instantly
  4. Permission: Only visible to people who have permission to edit

## Dashboard

- **Featueres**
  1. View-only: Cannot edit visuals, filters, or layout (unnless you're the owner)
  2. Refresh Options: Scheduled SPICE refresh or live query (depending on setup)
  3. Stable snapshot: Dashboards don't change unnless republished from the Analysis

## Row-Level Security

- In the Enterprise edition of **Amazon QuickSight**, you can restrict access to a dataset by configuring **row-level security** (**RLS**) on it. You can do this before or after you have shared the dataset. When you share a dataset with **RLS** with dataset owners, they can still see all the data. When you share it with **readers**, however, they can only see the data restricted by the permission dataset rules.
- Also, when you embed **Amazon QuickSight** dashboards in your application for unregistered users of QuickSight, you can use **row-level security** (**RLS**) with tags. In this case, you use tags to specify which data your users can see in the dashboard depending on who they are.
- You can restrict access to a dataset using **username** or **group-based rules**, **tag-based rules**, or both.
- **Remarks**:
  - Choose **user-based rules** if you want to secure data for users or groups provisioned (**registered**) in **QuickSight**. To do so, select a permissions dataset that contains rules set by columns for each user or group accessing the data. Only users or groups identified in the rules have access to data.
  - Choose **tag-based rules** only if you are using embedded dashboards and want to secure data for users not provisioned (unregistered users) in QuickSight. To do so, define tags on columns to secure data. Values to tags must be passed when embedding dashboards.

## Pricing

- **Reader**:
  - A **Reader** in QuickSight is a user who:
    1. Views dashboards and reports
    2. Interacts with filters, drill-downs, visuals, e.t.c
    3. Cannot create or publish dashboards

## Q in QuickSight Dashboard Q&A

- **Dashboard Q&A** by **Amazon Q** in **QuickSight** enables **QuickSight Authors** to add **Data Q&A** to their dashboards in one-click. With **dashboard Q&A**, **QuickSight** users can ask and answer questions about their data using natural language. When you turn on **dashboard Q&A**, you can choose which datasets to use for **dashboard Q&A** to ensure that your end users get the answers they need.
- **Dashboard Q&A** uses the data values that are rendered on the dashboard. End users can ask for different slices of the same data that they see on the dashboard. For example, the dashboard might include a KPI visual that shows the month-over-month change in revenue, but the user might want to see the year-over-year change. The user can do this by asking a question that references the fields and values present on the dashboard. The user does not need to know the exact field and value names that are used in the raw data.
- Dashboard Q&A capabilities of Q in QuickSight automatically extract semantic information presented in dashboards and use it to enable Q&A over specific data and improves existing Topic based Q&A experiences by automatically using semantics from dashboards to improve Q&A answers. With Dashboard Q&A Authors can quickly deliver self-service access to customized data insights for the entire organization.

## Unique Key for Dataset

- **Unique Key for Dataset**, enable users to define additional aspects of their data semantics.

## Amazon Q in embedded QuickSight

- Amazon Q in QuickSight brings generative AI to business intelligence, transforming how employees interact with data through natural language capabilities like AI-powered executive summaries, customizable data stories, multi-visual data Q&A experience, and scenarios capability for advanced data analysis without specialized skills.
- The Generative BI capabilities of **Amazon Q in QuickSight** help business analysts and business users easily build and consume insights using natural language.
- With executive summaries, users of embedded dashboards can quickly grasp essential insights from any dashboard in seconds. Dashboard-authoring capabilities empower your users to build interactive dashboards more easily than ever, leveraging natural language to generate visuals and perform complex calculations with ease. With just a few lines of code, developers can integrate Generative BI capabilities into their applications by embedding the new multi-visual Q&A experience—enabling end users to confidently explore and answer questions from data. Users can prompt Amazon Q using a few words to generate a sharable document or presentation in moments that explains data, extracts key insights and visuals, and recommends best actions to improve your business.

# Resources and Further Reading

1. [AWS Release Note Release Note - Q in QuickSight Dashboard Q&A](https://aws.amazon.com/about-aws/whats-new/2025/01/q-quicksight-dashboard-q-a/?ref=dailydev)
2. [Documentation - Amazon QuickSight - User Guide - Turn on the Dashboard Q&A experience in Amazon QuickSight](https://docs.aws.amazon.com/quicksight/latest/user/dashboard-qa.html)
3. [Documentation - Amazon QuickSight - User Guide - Adding a unique key to an Amazon QuickSight dataset](https://docs.aws.amazon.com/quicksight/latest/user/set-unique-key.html)
4. [Amazon Q](https://aws.amazon.com/q/)
5. [Documentation - Amazon QuickSight - User Guide - Using row-level security in Amazon QuickSight](https://docs.aws.amazon.com/quicksight/latest/user/row-level-security.html)
6. [Documentation - Amazon QuickSight - User Guide - Customizing access to Amazon QuickSight capabilities](https://docs.aws.amazon.com/quicksight/latest/user/customizing-permissions-to-the-quicksight-console.html)
