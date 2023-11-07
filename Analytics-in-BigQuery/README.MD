# Analysis with BigQuery

![Google BigQuery Image](../images/Google-BigQuery.png.png)

## Table Of Contents

- [Further Reading]()
  1. []()

# Introduction to Google BigQuery

- Google BigQuery is a fully managed, serverless enterprise data warehouse that enables scalable analysis over petabytes of data. It is a Platform as a Service that supports querying using a dialect of `SQL`. It also has built-in machine learning capabilities. 
- **BigQuery** is designed to make it easy to ingest, store, and analyze data from a variety of sources, including databases, logs, and social media. It can be used to power a wide range of applications, including business intelligence, customer analytics, and fraud detection.

## Advantages of Google BigQuery

- **Scalability**: BigQuery can handle petabytes of data without requiring any infrastructure management. This makes it ideal for organizations that are dealing with large and growing datasets.
- **Speed**: BigQuery can query billions of rows of data in seconds. This makes it possible to get real-time insights from your data.
- **Cost-effectiveness**: BigQuery's serverless architecture means that you only pay for the resources that you use. This makes it a cost-effective solution for organizations of all sizes.
- **Ease of use**: BigQuery supports standard SQL, so it is easy to use for analysts and data scientists who are already familiar with SQL.
- **Machine learning**: BigQuery has built-in machine learning capabilities that can be used to analyze data and make predictions.
- **Integration with other Google Cloud services**: BigQuery integrates with other Google Cloud services, such as Looker Studio, Looker, and Google Analytics. This makes it easy to visualize and analyze your data.

# Join Operations

## Join Operation: `inner join` or `join`

- An `INNER JOIN`, or simply `JOIN`, effectively calculates the Cartesian product of the two from_items and discards all rows that don't meet the join condition.

```sql
with
table_a as (
            select 1 as w, 'a' as x
            union all (select 2 as w, 'b' as x )
            union all (select 3 as w, 'c' as x )
            union all (select 3 as w, 'd' as x )
            ),
table_b as (
            select 2 as y, 'k' as z
            union all (select 3 as y, 'm' as z )
            union all (select 3 as y, 'n' as z )
            union all (select 4 as y, 'p' as z )
            )
select table_a.*, table_b.y, table_b.z
from table_a
join table_b on table_a.w = table_b.y
```
