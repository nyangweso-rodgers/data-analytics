--------------- Market Activator Assignemtns, Market Activators ------------------
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
                                      ),
--------------------- Market Activators ----------------------
market_activators as (
                      select *,
                      row_number()over(partition by id order by bq_upload_time desc) as index
                      from `karuru_reports.market_activators`
                      where date(created_at) >= '2023-08-01'
                    ),
market_activators_cte as (
                          select distinct created_at,
                          id,
                          names,
                          email,
                          msisdn,
                          market_id,
                          active
                          from market_activators
                          where index = 1
                          ),
mashup as (
          select maa.id as market_activator_assignment_id,
          --maa.valid_from,
          date(maa.valid_from) as valid_from_date,
          --maa.valid_to,
          date(maa.valid_to) as valid_to_date,
          maa.route_id,
          maa.assignment_by,
          maa.de_assignment_by,
          maa.market_activator_id,
          ma.names as market_activator_names,
          ma.email as market_activator_email,
          ma.msisdn as market_activator_msisdn,
          ma.active as active_market_activator
          from market_activator_assignments_cte maa
          left join market_activators_cte ma on maa.market_activator_id = ma.id
          )
select * from mashup
where route_id = '0CW5XSGTW5RSX'