# Analysis with BigQuery

## Table Of Contents

# Join Operations

## Join Operation: `inner join` or `join`
* An `INNER JOIN`, or simply `JOIN`, effectively calculates the Cartesian product of the two from_items and discards all rows that don't meet the join condition. 
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
