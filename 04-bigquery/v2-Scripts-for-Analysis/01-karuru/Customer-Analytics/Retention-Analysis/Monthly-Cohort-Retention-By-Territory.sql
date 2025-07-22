---------------- Karuru ---------
----------- Monthly Cohort Retention Test Script ----------
----------- By Territory -------------
with
monthly_dns as (
                SELECT distinct date_trunc(delivery_date, month) as delivery_month,
                country_code,
                territory_id,
                outlet_id,
                sum(total_delivered) as total_delivered
                FROM `kyosk-prod.karuru_scheduled_queries.karuru_dns_daily_revenue` 
                WHERE delivery_date < current_date
                and country_code = 'UG' and territory_id = 'Kawempe'
                group by 1,2,3,4
                ),
outlets_summary as (
                    select distinct outlet_id,
                    first_value(delivery_month)over(partition by outlet_id order by delivery_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_delivery_month,
                    first_value(total_delivered)over(partition by outlet_id order by delivery_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_delivered, 
                    --last_value(delivery_month)over(partition by outlet_id order by delivery_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_delivery_month,
                    last_value(territory_id)over(partition by outlet_id order by delivery_month ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as territory_id,
                    country_code
                    from monthly_dns
                    order by outlet_id
                    ),
cohort_size as (
                select distinct country_code, 
                territory_id,
                first_delivery_month as cohort_year_month,
                extract(year from first_delivery_month) as cohort_year,
                extract(month from first_delivery_month) as cohort_month,
                count(1) as cohort_size_outlets,
                sum(first_total_delivered) as cohort_size_gmv
                from outlets_summary
                group by 1,2,3,4,5
                order by country_code, territory_id,cohort_year, cohort_month
                ),
outlets_activites as (
                      select distinct outlets_summary.country_code,
                      outlets_summary.territory_id,
                      monthly_dns.outlet_id,
                      date_diff(monthly_dns.delivery_month, outlets_summary.first_delivery_month, month) as month_number,
                      extract(year from outlets_summary.first_delivery_month) as cohort_year,
                      extract(month from outlets_summary.first_delivery_month) as cohort_month,
                      sum(monthly_dns.total_delivered) as monthly_gmv
                      from monthly_dns
                      left join outlets_summary on monthly_dns.outlet_id = outlets_summary.outlet_id
                      group by 1,2,3,4,5,6
                      order by country_code, territory_id,monthly_dns.outlet_id, month_number
                      ),
cohort_retention as (
                      select distinct outlets_activites.country_code,
                      outlets_activites.territory_id,
                      outlets_activites.cohort_year,
                      outlets_activites.cohort_month,
                      outlets_activites.month_number,
                      count(outlets_activites.outlet_id) as count_of_outlets,
                      cohort_size.cohort_year_month,
                      cohort_size.cohort_size_outlets,
                      cohort_size.cohort_size_gmv
                      from outlets_activites
                      left join cohort_size on outlets_activites.cohort_year = cohort_size.cohort_year and outlets_activites.cohort_month = cohort_size.cohort_month and outlets_activites.territory_id = cohort_size.territory_id
                      group by 1,2,3,4,5,7,8,9
                      order by country_code, territory_id, cohort_year, cohort_month, month_number
                      )
select * from cohort_retention