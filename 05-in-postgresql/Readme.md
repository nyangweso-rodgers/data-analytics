# Analytics in PostgreSQL

## Table Of Contents

# Date Functions

## 1. `date_trunc()` Functions

### 1.1 Get Week Start from `datetime`

```sql
    select distinct
    cast(date_trunc('week', created_at) as date) as week_start
    from <table_name>
```

# Resources and Further Reading
