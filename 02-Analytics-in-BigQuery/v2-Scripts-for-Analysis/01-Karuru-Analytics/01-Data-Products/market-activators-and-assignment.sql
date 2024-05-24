with
market_activator_assignments as (
                                SELECT *,
                                row_number()over(partition by route_id order by valid_from desc) as index
                                FROM `kyosk-prod.karuru_reports.market_activator_assignments` 
                                WHERE date(valid_from) >= '2023-08-01' 
                              ),
market_activator_assignment_report as (
                                      select distinct id as market_activator_assignment_id,
                                      market_activator_id,
                                      route_id,
                                      valid_from,
                                      valid_to,
                                      assignment_by
                                      from market_activator_assignments
                                      --where index = 1
                                    ),
market_activators as (
                      select *,
                      row_number()over(partition by id order by bq_upload_time desc) as index
                      from `karuru_reports.market_activators`
                      where date(created_at) >= '2023-08-01'
                    ),
market_activators_report as (
                              select distinct id,
                              names,
                              email,
                              msisdn,
                              market_id,
                              active,
                              created_at
                              from market_activators
                              --where index = 1
                              --and names not in ('Test_Luzira MD')
                              ),
market_activators_with_assignement as (
                                        select mar.*,
                                        maar.*
                                        from market_activators_report mar
                                        left join market_activator_assignment_report maar on mar.id = maar.market_activator_id
                                        )
select * from market_activators_with_assignement
--where msisdn like '234%'
where email = 'clarinaalbert@gmail.com'
order by id