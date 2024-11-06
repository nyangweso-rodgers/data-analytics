# Using MongoDB Compass

## Table Of Contents

# Using `$in`

- Example:
  - Get records from MongoDB in a given lists:
    ```sql
        #
        {id: {$in: ["a", "b", "c", "d"]}}
    ```

# Search

## Exact Search

```sql
  {first_name:"Rodgers"};
```

- `first_name` with exact match of `Rodgers`

## Contains Search

```sql
  {first_name:/Rodg/i};
```

- `first_name` **CONTAINS** the phrase `Rodg`

# Regular Expression (regex) Operator `$regex`

## 1.

- In MongoDB, the equivalent of SQL's `STARTS_WITH` can be achieved using the regex operator `$regex` to find documents where a field starts with a particular character or substring.
- Example 1:
  - Here is an example query in MongoDB to find records where a field starts with a particular character (e.g., records where the name field starts with "A"):
    ```sql
      db.collection.find({ "name": { $regex: /^A/ } })
    ```

# Resources and Further Reading
