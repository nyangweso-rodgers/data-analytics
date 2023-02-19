------------ PostgreSQL ---------------
with 
flats_table as (
    select *, 
    ROW_NUMBER()OVER(PARTITION BY city ORDER BY price ASC) as index
    from flats
)

select distinct id, city, price from flats_table
where index <= 3
ORDER BY id