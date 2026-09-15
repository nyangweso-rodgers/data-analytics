WITH
collection_officer_assignments_cte as (
    SELECT * 
    FROM reporting_service.collection_officer_assignments
    ORDER BY sync_at desc
    )
select *
from collection_officer_assignments_cte
where accountRef = '41933017'