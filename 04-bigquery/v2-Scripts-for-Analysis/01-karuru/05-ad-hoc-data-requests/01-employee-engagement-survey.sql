---------------------- 2024 Employee Engagement Survey ---------------------------
---------------------------- Created By - Rodgers Nyangweso ------------------------------
with
employee_engagement_survey as (
                                SELECT *, row_number()over(order by Timestamp) as id 
                                #FROM `kyosk-prod.google_sheets.employee_engagement_survey` # live google sheets
                                FROM `kyosk-prod.google_sheets.employee_engagement_survey_table` # flat table 
                                ),
employee_engagement_survey_cte as (
                                  select distinct id, 
                                  Please_select_your_department,
                                  Gender__Please_select_your_gender_,
                                  Length_of_Service__Please_select_your_length_of_service__Effective_first_date_of_employment_at_Kyosk_,
                                  Contract_Type__Please_select_your_contract_engagement_type,
                                  Who_are_the_people_you_collaborate_with_the_most_,
                                  Describe_the_Kyosk_culture_in_one_word_,
                                  What_should_we_keep_doing_,
                                  What_should_we_start_doing__,
                                  What_should_we_stop_doing_,
Country_or_Business_Unit__Please_select_your_Country_or_Business_Unit__NB__All_employees_wthin_Tech___Product_and_Global_Dept_should_select_the_Business_Unit_irrespective_of_their_location_and_as_indicated__for_analysis_purposes_,
Country_or_Business_Unit__Please_select_your_Country_or_Business_Unit__NB__All_employees_wthin_Tech___Product_and_Global_Dept_should_select_the_Business_Unit_irrespective_of_their_location_and_as_indicated__for_analysis_purposes_ as country_or_business_unit,
                                  dimension_type,
                                  dimension_value
                                  from employee_engagement_survey
                                  UNPIVOT (
                                    dimension_value FOR dimension_type IN (
                                      __I_am_aware_of_Kyosk_s_policies_and_SOP_s__Standard_Operating_Procedures__,
                                      __I_have_access_to_Kyosk_s_HR_policies_and_I_can_refer_to_them_at_anytime__, 
                                      __I_have_access_to_Kyosk_s_Operational_Functional_policies_and_I_can_refer_to_them_at_anytime__,
                                      __I_have_the_psychological_safety_to_report_policy_violations_without_fear_of_retaliation__,
                                      __I_know_the_procedure_for_whistleblowing_or_reporting_policy_violations__,
                                      __I_have_a_clear_understanding_of_my_roles___responsibilities__,
                                      __I_receive_continous_feedback_that_helps_me_improve_my_performance_,
                                      __At_Kyosk__people_are_held_accountable_for_their_performance_,
                                      __I_have_monthly_performance_dialogue_sessions_with_my_manager_supervisor__,
                                      __I_feel_my_manager_supports__hand_holds_and_directs_me_to_effectively_execute_on_my_duties__,
                                      __I_feel_that_my_career_goals_can_be_met_at_Kyosk__,
                                      __Kyosk_provides_me_with_the_opportunity_for_learning__development_and_personal_growth_,
                                      __I_receive_encouragement_and_opportunities_from_my_manager_to_develop_my_career__,
                                      __My_manager_promotes_teamwork_as_a_value__,
                                      __I_get_opportunities_to_collaborate_at_work__,
                                      __I_receive_the_information_I_need_to_do_my_job_effectively__,
                                      __There_is_transparency_with_information_across_Kyosk__,
                                      __There_is_open_and_honest_communication_at_Kyosk_,
                                      __I_feel_like_I_am__In_the_Know__of_what_is_happening_at_Kyosk__,
                                      __I_have_confidence_in_the_senior_leadership_team_to_make_the_right_decisions_for_Kyosk__,
                                      __The_behavior_of_our_senior_leadership_team_is_consistent_with_Kyosk_s_values__,
                                      __Our_leaders_put_as_much_energy_and_investment_into_their_people_as_they_put_into_achieving_the_business_goals__,
                                      __I_am_optimistic_about_the_future_of_Kyosk_headed_by_our_current_leaders__,
                                      __I_feel_my_diverse_perspectives_are_valued_and_encouraged_in_my_team__,
                                      __I_am_comfortable_voicing_my_ideas_and_opinions__even_if_they_are_different_from_others__,
                                      __I_feel_my_team_is_diverse_enough__mix_of_age__work_experience__background__to_deliver_different_ideas_and_ways_of_thinking__,
                                      __At_Kyosk__customer_problems_and_concerns_are_dealt_with_quickly__,
                                      __I_am_empowered_to_make_decisions_to_best_serve_both_my_internal_and_external_customers____Internal_customers__colleagues_,
                                      __I_feel_able_to_provide_support_to_customers_with_my_current_level_of_training_and_knowledge_of_the_business__,
                                      __I_feel_employees_at_Kyosk_exemplify_our_values__,
                                      __I_would_likely_recommend_Kyosk_to_friends_as_a_great_place_to_work__employer_of_choice__,
                                      __I_am_satisfied_with_the_overall_job_security_at_Kyosk__,
                                      __I_believe_I_can_reach_my_full_potential_at_Kyosk__,
                                      __I_feel_like_I_am_treated_with_fairness_and_respect_at_work__,
                                      __I_enjoy_my_role__,
                                      __My_work_gives_me_a_feeling_of_personal_accomplishment__,
                                      __I_am_proud_to_work_for_Kyosk__,
                                      __I_feel_satisfied_with_the_balance_between_my_work_and_other_aspects_of_my_life_,
                                      __I_feel_Kyosk_cares_about_its_employees_well_being_beyond_work__,
                                      __I_am_aware_of_all_the_non_cash_benefits_that_are_available_to_employees_at_Kyosk__Group_Life_Cover__WIBA_Plus___Medical_Insurance__,
                                      __I_feel_the_work_environment_is_conducive_for_me_to_deliver_effectively__,
                                      __I_receive_recognition_from_my_manager_when_I_perform_excellently__,
                                      __I_feel_my_remuneration__salary__is_commensurate_to_on_my_role__,
                                      __I_feel_Kyosk_s_employment_benefits__non_cash_benefits__are_appropriate_are_commensurate_to_my_job__,
                                      __I_am_confident_that_action_will_be_taken_as_a_result_of_this_survey__,
                                      __I_believe_that_positive_change_will_happen_as_a_result_of_this_survey_
                                      )
                                  )
                                  ),
employee_engagement_survey_with_dimension as (
                      select *except(dimension_type),
                      case when (dimension_value = 'Strongly Agree') then 1 else 0 end as strongly_agree_score,
                      case when (dimension_value = 'Agree') then 1 else 0 end as agree_score,
                      case when (dimension_value = 'Neutral') then 1 else 0 end as neutral_score,
                      case when (dimension_value = 'Disagree') then 1 else 0 end as disgree_score,
                      case when (dimension_value = 'Strongly Disagree') then 1 else 0 end as strongly_disgree_score,
                      REPLACE(dimension_type, "_", ' ') as dimension_type,
                      case
                        # Ethics & Compliance
                        when dimension_type =  "__I_am_aware_of_Kyosk_s_policies_and_SOP_s__Standard_Operating_Procedures__"then "Ethics & Compliance"
                        when dimension_type =  "__I_have_access_to_Kyosk_s_HR_policies_and_I_can_refer_to_them_at_anytime__"then "Ethics & Compliance"
                        when dimension_type =  "__I_have_access_to_Kyosk_s_Operational_Functional_policies_and_I_can_refer_to_them_at_anytime__"then "Ethics & Compliance"
                        when dimension_type =  "__I_have_the_psychological_safety_to_report_policy_violations_without_fear_of_retaliation__"then "Ethics & Compliance"
                        when dimension_type =  "__I_know_the_procedure_for_whistleblowing_or_reporting_policy_violations__"then "Ethics & Compliance"

                        #emloyee engagement
                        when dimension_type =  "__I_enjoy_my_role__"then "Employee Engagement"
                        # Performance & Accountability
                        when dimension_type =  "__I_have_a_clear_understanding_of_my_roles___responsibilities__"then "Performance & Accountability"
                        when dimension_type =  "__I_receive_continous_feedback_that_helps_me_improve_my_performance_"then "Performance & Accountability"
                        when dimension_type =  "__At_Kyosk__people_are_held_accountable_for_their_performance_"then "Performance & Accountability"
                        when dimension_type =  "__I_have_monthly_performance_dialogue_sessions_with_my_manager_supervisor__"then "Performance & Accountability"
                        when dimension_type =  "__I_feel_my_manager_supports__hand_holds_and_directs_me_to_effectively_execute_on_my_duties__"then "Performance & Accountability"

                        # Personal Growth & Career Development
                        when dimension_type =  "__I_feel_that_my_career_goals_can_be_met_at_Kyosk__"then "Personal Growth & Career Development"
                        when dimension_type =  "__Kyosk_provides_me_with_the_opportunity_for_learning__development_and_personal_growth_"then "Personal Growth & Career Development"
                        when dimension_type =  "__I_receive_encouragement_and_opportunities_from_my_manager_to_develop_my_career__"then "Personal Growth & Career Development"
  
                        # Collaboration
                        when dimension_type =  "__My_manager_promotes_teamwork_as_a_value__"then "Collaboration"
                        when dimension_type =  "__I_get_opportunities_to_collaborate_at_work__"then "Collaboration"
                        when dimension_type =  "__I_receive_the_information_I_need_to_do_my_job_effectively__"then "Collaboration"

                        # Communications
                        when dimension_type =  "__There_is_transparency_with_information_across_Kyosk__"then "Communications"
                        when dimension_type =  "__There_is_open_and_honest_communication_at_Kyosk_"then "Communications"
                        when dimension_type =  "__I_feel_like_I_am__In_the_Know__of_what_is_happening_at_Kyosk__"then "Communications"

                        # Value Driven Leadership
                        when dimension_type =  "__I_have_confidence_in_the_senior_leadership_team_to_make_the_right_decisions_for_Kyosk__"then "Value Driven Leadership"
                        when dimension_type =  "__The_behavior_of_our_senior_leadership_team_is_consistent_with_Kyosk_s_values__"then "Value Driven Leadership"
                        when dimension_type =  "__Our_leaders_put_as_much_energy_and_investment_into_their_people_as_they_put_into_achieving_the_business_goals__"then "Value Driven Leadership"
                        when dimension_type =  "__I_am_optimistic_about_the_future_of_Kyosk_headed_by_our_current_leaders__"then "Value Driven Leadership"

                        # Diversity & Inclusion
                        when dimension_type =  "__I_feel_my_diverse_perspectives_are_valued_and_encouraged_in_my_team__"then "Diversity & Inclusion"
                        when dimension_type =  "__I_am_comfortable_voicing_my_ideas_and_opinions__even_if_they_are_different_from_others__"then "Diversity & Inclusion"
                        when dimension_type =  "__I_feel_my_team_is_diverse_enough__mix_of_age__work_experience__background__to_deliver_different_ideas_and_ways_of_thinking__"then "Diversity & Inclusion"

                        # Customer Focus
                        when dimension_type =  "__At_Kyosk__customer_problems_and_concerns_are_dealt_with_quickly__"then "Customer Focus"
                        when dimension_type =  "__I_am_empowered_to_make_decisions_to_best_serve_both_my_internal_and_external_customers____Internal_customers__colleagues_"then "Customer Focus"
                        when dimension_type =  "__I_feel_able_to_provide_support_to_customers_with_my_current_level_of_training_and_knowledge_of_the_business__"then "Customer Focus"

                        # Kyosk Culture & Values
                        when dimension_type =  "__I_feel_employees_at_Kyosk_exemplify_our_values__"then "Kyosk Culture & Values"
                        when dimension_type =  "__I_would_likely_recommend_Kyosk_to_friends_as_a_great_place_to_work__employer_of_choice__"then "Kyosk Culture & Values"
                        when dimension_type =  "__I_am_satisfied_with_the_overall_job_security_at_Kyosk__"then "Kyosk Culture & Values"
                        when dimension_type =  "__I_believe_I_can_reach_my_full_potential_at_Kyosk__"then "Kyosk Culture & Values"
                        when dimension_type =  "__I_feel_like_I_am_treated_with_fairness_and_respect_at_work__"then "Kyosk Culture & Values"

                        # Employee Engagement
                        when dimension_type =  "__My_work_gives_me_a_feeling_of_personal_accomplishment__"then "Employee Engagement"
                        when dimension_type =  "__I_am_proud_to_work_for_Kyosk__"then "Employee Engagement"
                        when dimension_type =  "__I_feel_satisfied_with_the_balance_between_my_work_and_other_aspects_of_my_life_"then "Employee Engagement"

                        # Employee Well-Being
                        when dimension_type =  "__I_feel_Kyosk_cares_about_its_employees_well_being_beyond_work__"then "Employee Well-Being"
                        when dimension_type =  "__I_am_aware_of_all_the_non_cash_benefits_that_are_available_to_employees_at_Kyosk__Group_Life_Cover__WIBA_Plus___Medical_Insurance__"then "Employee Well-Being"
                        when dimension_type =  "__I_feel_the_work_environment_is_conducive_for_me_to_deliver_effectively__"then "Employee Well-Being"

                        # Reward, Recognition & Compensation
                        when dimension_type =  "__I_receive_recognition_from_my_manager_when_I_perform_excellently__"then "Reward, Recognition & Compensation"
                        when dimension_type =  "__I_feel_my_remuneration__salary__is_commensurate_to_on_my_role__"then "Reward, Recognition & Compensation"
                        when dimension_type =  "__I_feel_Kyosk_s_employment_benefits__non_cash_benefits__are_appropriate_are_commensurate_to_my_job__"then "Reward, Recognition & Compensation"

                        # Survey Follow Up
                        when dimension_type =  "__I_am_confident_that_action_will_be_taken_as_a_result_of_this_survey__"then "Survey Follow Up"
                        when dimension_type =  "__I_believe_that_positive_change_will_happen_as_a_result_of_this_survey_"then "Survey Follow Up"
                      else 'UNSET' end as dimension
                      from employee_engagement_survey_cte
                      ),
--------------------------- User Access Management ----------------
employee_engagement_survey_user_access as (
                                            SELECT distinct id,
                                            user_email,
                                            user_access,
                                            acces_status
                                            #FROM `kyosk-prod.google_sheets.employee_engagement_survey_user_access` 
                                            FROM `kyosk-prod.google_sheets.employee_engagement_survey_user_access_table`
                                            where acces_status = true
                                            order by user_email
                                            ) 
select eeswd.*, (strongly_agree_score + agree_score + neutral_score + disgree_score + strongly_disgree_score) as total_dimension_value_score,
eesua.user_email
from employee_engagement_survey_with_dimension eeswd
left join employee_engagement_survey_user_access eesua on eeswd.country_or_business_unit = eesua.user_access
where REGEXP_CONTAINS(eesua.user_email,@DS_USER_EMAIL)
--where id = 1
--group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19