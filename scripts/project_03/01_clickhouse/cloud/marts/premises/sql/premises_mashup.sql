WITH
--------------------- Premises ----------------------------------
premises_cte AS
(
    SELECT *
    FROM
    (
        SELECT distinct id,
        premise_name,
        customer_id,
        premise_type_id,
        substate_id,
        town,
        row_number() OVER (partition by customer_id ORDER BY updated_at DESC) as rnk 
        from fma.premises
    ) WHERE rnk = 1
),
--------------------- Premise Details ----------------------------------
premise_details_cte AS
    (
        SELECT *
        FROM
        (
            SELECT distinct premise_id,
            village,
            subcounty,
            parish,
            latitude,
            longitude,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            from fma.premise_details
        ) WHERE rnk = 1
    ),
--------------------- Substates ----------------------------------
subcounty_cte AS
    (
        SELECT *
        FROM
        (
            SELECT distinct id,
            substate_type,
            substate_name,
            state_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            from fma.substates
        ) WHERE rnk = 1
        and substate_type =  'Sub County'
    ),
--------------------- States ----------------------------------
states_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            state_name,
            state_type,
            region_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.states
        ) WHERE rnk = 1
    ),
--------------------- Regions ----------------------------------
regions_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            region_name,
            country_id,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.regions
        ) WHERE rnk = 1
    ),
--------------------- Countries ----------------------------------
countries_cte AS
    (
        SELECT *
        FROM (
            SELECT distinct id,
            country_name,
            row_number() OVER (partition by id ORDER BY updated_at DESC) as rnk 
            FROM fma.countries
        ) WHERE rnk = 1
        ),
--------------------- Premises Mashup ----------------------------------
premises_mashup_cte as (
    select *
    from (
        select distinct premises_cte.customer_id as customer_id,
    countries_cte.country_name as country_name,
    regions_cte.region_name as region_name,
    --state_type,
    states_cte.state_name AS county,
    subcounty_cte.substate_name AS subcounty,
    premise_details_cte.village,
    premise_details_cte.parish,
    premise_details_cte.latitude,
    premise_details_cte.longitude
    from premises_cte
    left join premise_details_cte on premise_details_cte.premise_id = premises_cte.id
    left join subcounty_cte on subcounty_cte.id = premises_cte.substate_id
    left join states_cte on states_cte.id = subcounty_cte.state_id
    left join regions_cte on regions_cte.id = states_cte.region_id
    left join countries_cte on countries_cte.id = regions_cte.country_id
    ) where country_name = 'Kenya'
)
select --*
distinct state_type
--distinct country_name, region_name, county, subcounty
from states_cte
--from substates_cte
--from premises_mashup_cte
--ORDER BY 1,2,3,4
limit 1000