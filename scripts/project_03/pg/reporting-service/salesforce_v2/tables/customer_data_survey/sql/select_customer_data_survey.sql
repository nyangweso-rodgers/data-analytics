with
customer_data_survey_cte as (
	SELECT id, 
	--ownerid, isdeleted, name, createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, 
	number_of_cows, number_of_goats, been_living_in_the_same_location_for, 
	--next_of_kin_name, next_of_kin_surname, 
	number_of_pigs, 
	--share_name_phone_of_next_of_kin, next_of_kin_phone_number, 
	number_of_sheep, 
	--sales_manager, lead_name, 
	account, 
	--own_the_land_for_product_installation, 
	how_long_have_you_been_a_farmer, 
	do_you_have_animals, 
	--rm_no_sales_telesales_dealer_agent, 
	type_of_fruits_and_vegetables_grown, pest_disease_control_pest_type_usage, other_machinery_and_equipment_ownership, salary_amount, 
	main_purpose_of_acquiring_the_product, primary_decision_maker_to_buy_product, main_source_of_income, periodicity_of_payment, 
	--compensation_for_the_help_provided, duration_of_this_activity, 
	total_amount_from_pension, total_amount_from_salary_government, 
	--credit_check_result, other_sources_of_income_of_the_household, 
	--credit_score, # all NULL
	rate_how_well_the_sa_was_able_to_educate, of_kenyans_that_pay_suppliers_on_time, of_kenyans_ever_defaulted_on_a_loan, overall_risk_assessment, 
	--relation_manager, 
	--types_of_crops_grown, # all NULL
	how_long_have_you_had_the_same_phone_no, 
	--county_in_which_household_resides, l
	living_in_the_same_location_for, 
	--relation_with_next_of_kin, number_of_household_members, ready_to_make_the_deposit_on, 
	amount_left_after_monthly_expenses, 
	--type_of_crops_grown, # all NULL 
	--crop_grown, # all NULL 
	--wheat_sale, maize_sale, paddy_sale, irish_potatoes_sale, cassava_sale, beans_sale, sunflower_sale, groundnuts_sale, other_stable_field_crops_sale, wheat_self_consumption, maize_self_consumption, 
	--paddy_self_consumption, irish_potatoes_self_consumption, cassava_self_consumption, beans_self_consumption, sunflower_self_consumption, groundnuts_self_consumption, 
	--other_staple_field_crop_self_consumption, tomatoes_sale, cabbage_sale, spinach_sale, onion_sale, green_beans_sale, other_fruits_and_vegetable_sale, 
	--tomatoes_self_consumption, cabbage_self_consumption, spinach_self_consumption, onion_self_consumption, green_beans_self_consumption, other_fruit_vegetable_self_consumption, 
	harvest_cycle_per_year, 
	--same_number_as_provided_in_cds1, recall_the_call, 
	farm_acreage, main_purpose_of_acquiring_the_pump, 
	--how_did_you_hear_about_sunculture_pumps, 
	other_sources_of_water, 
	hours_spent_fetching_water_every_week, amount_paid_for_getting_water_each_week, amount_paid_for_water_other_pump_usage, 
	--customer_data_survey_number, 
	quantity_of_water_usage_per_week, water_tank_capacity, electricity_connectivity, number_of_financial_dependants, number_of_working_age_adults_in_the_hh, 
	--number_of_pensioners_living_in_the_hh, # all NULL
	--no_of_family_members_friends_helping, # all NULL
	--number_of_businesses_before, # all NULL
	average_monthly_income, 
	--understand_the_products_shared_with_you, 
	no_of_years_working_with_same_employer, no_of_jobs_in_the_last_5yrs, total_amount_from_remittances, total_amount_from_agriculture, total_amount_from_commerce_and_trade, total_amt_from_salary_private_inst, total_amount_from_provision_of_services,
	--agent_seek_to_understand_how_you_source, 
	amount_spent_on_school_fees, amount_spent_on_food, amount_spent_on_farm_inputs, amount_spent_on_rent, amount_spent_on_loans, amount_spent_on_other, currently_have_any_outstanding_loans, 
	total_amount_of_outstanding_loan_s, no_of_months_to_finish_paying_loan_s, preferred_banking_method, 
	--world_perception_supplier, 
	--world_perception_default, 
	number_of_loans_taken_in_the_last_2yrs, 
	periodicity_of_the_income, 
	--like_further_clarification_on_our_prod, # all NULL 
	--likelihood_to_recommend_our_prod, # all NULL
	--name_number_of_the_recommend_client, # all NULL
	--agent, 
	--survey_completed, # all NULL 
	stage, cds1tracker, 
	birth_date, date_of_birth, 
	--email, 
	national_id_number, 
	--gender, 
	mobile_number, 
	--rm_name, 
	--cds_source, # all NULL
	--did_you_previously_own_a_water_pump, # all NULL
	--carbon_credit_which_pump_type, # all NULL
	cds1_date, cds2_date, 
	--number_of_outstanding_loans, # all NULL
	--depth_of_the_water_source, # all NULL
	lead_record
	--creditscore # all NULL
	--clients_county_ug, # all NULL 
	--clients_district, # all NULL 
	--clients_location_latitude_s, # all NULL
	--clients_location_longitude_s, # all NULL
	--clients_nearest_landmark, # all NULL 
	--clients_parish, # all NULL 
	--clients_sub_county, # all NULL
	--clients_village_ug, # all NULL
	--amount_spent_on_treatment_medical, # all null
	--reason_for_decline, # all NULL
	--seasonality_of_the_water_source, # all NULL
	--product, # all NULL 
	--do_you_use_agricultural_inputs, # all NULL
	--do_you_collaborate_with_other_farmers, # all NULL
	--do_you_keep_records_of_farm_performance, # all NULL
	--during_your_working_peak_season_how_man, # all NULL 
	--how_do_you_compensate_them_for_the_help, # all NULL
	--do_you_have_a_reliable_market_for_your_p, # all NULL
	--in_which_market_do_you_sell, # all NULL
	--what_is_your_main_source_of_knowledge, # all NULL
	--do_they_have_a_registered_business, # all NULL
	--what_type_of_crops_fruits_and_vegetables, # all NULL
	--amount_spent_on_transport,# all NULL 
	--amount_spent_on_medical_expenses # all NULL
	--_salesforce_id, _synced_at
	FROM salesforce_v2.customer_data_survey
	),
accounts_cte as (
	SELECT id, 
	--isdeleted, masterrecordid, 
	"name", "type", parentid, 
	--billingstreet, billingcity, billingstate, billingpostalcode, billingcountry, billinglatitude, billinglongitude, billinggeocodeaccuracy, 
	--shippingstreet, shippingcity, shippingstate, shippingpostalcode, shippingcountry, shippinglatitude, shippinglongitude, shippinggeocodeaccuracy, 
	phone, 
	--website, photourl, industry, numberofemployees, description, ownerid, 
	--createddate, createdbyid, lastmodifieddate, lastmodifiedbyid, systemmodstamp, lastactivitydate, lastvieweddate, lastreferenceddate, 
	--sourcesystemidentifier, 
	--ispartner, channelprogramname, channelprogramlevelname, 
	--jigsaw, jigsawcompanyid, 
	accountsource, sicdesc, customer_type, acreage, payment_method, water_source, 
	"location", account_number, status, customer_source_account, 
	credit_check_result_status, 
	credit_score, customer_amt_id, date_of_birth_lead, 
	amt_customer_id, 
	income_threshold_l, 
	daily_water_usage_litres_l, water_source_distance_meters_l, category, referral_name, referral_id, 
	referral_phone_number, payment_terms, total_dynamic_head_mtrs, regionid, gender, crb_score, paymentsms, payment_sms_sent, 
	country_code, 
	--xpayment_terms, agent_name, customer_county, customer_kra_pin, customer_to_claim_vat_account, alternative_phone_number, 
	creditcheck_dateupdated, 
	through_partner_customer, 
	new_credit_score, 
	new_credit_check_result_status 
	--_salesforce_id, _synced_at
	FROM salesforce_v2.account
),
cds_mashup_cte as (
	select customer_data_survey_cte.*,
	country_code,
	amt_customer_id,
	credit_score,
	credit_check_result_status,
	accounts_cte.new_credit_score,
	new_credit_check_result_status,
	coalesce(accounts_cte.new_credit_score, accounts_cte.credit_score) as final_credit_score,
	date(creditcheck_dateupdated) as credit_check_updated_date
	from customer_data_survey_cte
	left join accounts_cte on accounts_cte.id = customer_data_survey_cte.account
	),
-- ad-hoc data request
spv_requests_cte as (
	select distinct country_code,
	amt_customer_id,
	national_id_number,
	mobile_number,
	average_monthly_income,
	CASE
	    WHEN average_monthly_income IS NULL THEN 'No Data'
	    WHEN average_monthly_income = 0 THEN '0'
	    WHEN average_monthly_income > 0 AND average_monthly_income <= 20000 THEN '1 - 20,000'
	    WHEN average_monthly_income > 20000 AND average_monthly_income <= 40000 THEN '20,001 - 40,000'
	    WHEN average_monthly_income > 40000 AND average_monthly_income <= 100000 THEN '40,001 - 100,000'
	    WHEN average_monthly_income > 100000 AND average_monthly_income <= 300000 THEN '100,001 - 300,000'
	    WHEN average_monthly_income > 300000 THEN '300,000+ (Outliers)'
	END AS avg_monthly_income_band,
	--credit_score,
	--credit_check_result_status,
	--new_credit_score,
	--new_credit_check_result_status,
	final_credit_score,
	CASE
	    WHEN final_credit_score IS NULL THEN 'No Data'
	    WHEN final_credit_score >= 0 AND final_credit_score < 0.25 THEN '0 - 0.25'
	    WHEN final_credit_score >= 0.25 AND final_credit_score < 0.5 THEN '0.25 - 0.5'
	    WHEN final_credit_score >= 0.5 AND final_credit_score < 0.75 THEN '0.5 - 0.75'
	    WHEN final_credit_score >= 0.75 AND final_credit_score <= 1 THEN '0.75 - 1'
	ELSE 'Out of Range' END AS credit_score_band
	--credit_check_updated_date
	from cds_mashup_cte
	where country_code = '254' or country_code is NULL
	),
get_distribution_of_avg_monthly_income_cte as (
	SELECT
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE average_monthly_income IS NULL) AS null_count,
    COUNT(*) FILTER (WHERE average_monthly_income = 0) AS zero_count,
    MIN(average_monthly_income) AS min_value,
    MAX(average_monthly_income) AS max_value,
    AVG(average_monthly_income) AS mean_value,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY average_monthly_income) AS p25,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY average_monthly_income) AS median_p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY average_monthly_income) AS p75,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY average_monthly_income) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY average_monthly_income) AS p95,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY average_monthly_income) AS p99
	from spv_requests_cte
	)
select *
--distinct country_code, count(distinct national_id_number), min(average_monthly_income), max(average_monthly_income)
from customer_data_survey_cte
--from spv_requests_cte
--from get_distribution_of_avg_monthly_income_cte
--group by 1 order by 2 desc
where lead_record = '00QPz00000SpiuvMAB'