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
