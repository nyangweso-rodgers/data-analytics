--------------------- Market Activator Assignment -----------------
with
market_activator_assignments as (
                                SELECT *,
                                row_number()over(partition by id order by valid_from desc) as index
                                FROM `kyosk-prod.karuru_reports.market_activator_assignments` 
                                WHERE date(valid_from) >= '2022-08-01' 
                              ),
market_activator_assignments_cte as (
                                      select distinct id,
                                      market_activator_id,
                                      route_id,
                                      valid_from,
                                      valid_to,
                                      assignment_by,
                                      de_assignment_by
                                      from market_activator_assignments
                                      where index =1
                                      )
select * from market_activator_assignments_cte
where route_id = '0CW5XSGTW5RSX'