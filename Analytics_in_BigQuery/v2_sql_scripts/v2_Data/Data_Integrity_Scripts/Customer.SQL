with
customer_with_index as (
                        SELECT *,
                        row_number()over(partition by name order by modified desc) as index 
                        FROM `kyosk-prod.erp_reports.customer` 
                        ),
customer_list as (
                  select distinct date(creation) as creation_date,
                  company,
                  name,
                  kyosk_identity_code,
                  customer_name,
                  duka_longitude,
                  duka_latitude,
                  sale_district_code,
                  route_code,
                  territory,
                  owner_phone_number,
                  disabled,
                  agent_name,
                  from customer_with_index
                  where index = 1
                  order by 1
                  )
select count(distinct name) from customer_list
--where FORMAT_DATE('%Y%m%d', creation_date) between @DS_START_DATE and @DS_END_DATE