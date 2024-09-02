# Joins in BigQuery

# Full Outer Join

- The `FULL OUTER JOIN` returns all records when there is a match in left (table1) or right (table2) table records.
- Remark:
  - The `FULL OUTER JOIN` returns all matching records from both tables whether the other table matches or not. So, if there are rows in "Customers" that do not have matches in "Orders", or if there are rows in "Orders" that do not have matches in "Customers", those rows will be listed as well.
- Example in BigQuery

  ```sql
    -------------------- Full Outer Join -----------------
    with
    sales as (
            select 'A' as id
            union all (select 'B' as id)
            ),
    customer_registrations as (
                                select 'A' as id
                                union all (select 'C' as id)

                                )
    select distinct coalesce(s.id, r.id) as id from sales s
    full outer join customer_registrations r on s.id = r.id
  ```
