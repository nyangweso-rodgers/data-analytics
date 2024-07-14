------------- Market Activator Assignent, Market Activators ---------------------
with
-------------------- Market Activator Assignent -----------------
market_activator_assignments as (
                                SELECT *,
                                row_number()over(partition by id order by valid_from desc) as index
                                FROM `kyosk-prod.karuru_reports.market_activator_assignments` 
                                WHERE date(valid_from) >= '2023-08-01' 
                              ),
market_activator_assignments_list as (
                                        select route_id,
                                        market_activator_id,
                                        date(valid_from) as valid_from,
                                        date(valid_to) as valid_to
                                        from  market_activator_assignments
                                        where index =1
                                        ),
--------------------------- Market Activators -----------------
market_activators as (
                      select *,
                      row_number()over(partition by id order by bq_upload_time desc) as index
                      from `karuru_reports.market_activators`
                      where date(created_at) >= '2023-08-01'
                    ),
market_activators_list as (
                          select distinct id,
                          active,
                          email,
                          names,
                          msisdn,
                          market_id,
                          from market_activators
                          where index =1
                          ),
------------------ Mashup --------------------------------------
market_activators_assignment_report as (
                                        select maal.* ,
                                        mal.active,
                                        mal.email,
                                        mal.market_id
                                        from market_activator_assignments_list maal
                                        left join market_activators_list mal on maal.market_activator_id = mal.id
                                        )
select *
from market_activators_assignment_report
where route_id = '0CW5Y2F5NETG1'
order by market_activator_id, valid_from