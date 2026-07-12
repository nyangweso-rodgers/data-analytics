with
--------------------- View Types ----------------------------------
view_types_cte as (
        select *
        from (
                SELECT id,
                name,
        row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
        FROM amt.view_types
        ) where rnk = 1
        ),