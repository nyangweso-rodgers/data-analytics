-------------- Monthly Credit Analytics ---------
--------------------- Created By - Rodgers Nyangweso ------------------------
with
territory_region_mapping as (
                              SELECT distinct country,
                              --region,
                              --sub_region,
                              --division,
                              original_territory_id,
                              new_territory_id,
                              coalesce(territory_renamed, territory) as territory_renamed
                              FROM `kyosk-prod.karuru_upload_tables.territory_region_mapping` 
                              ),   
montly_date_variables as (
                          SELECT distinct my_month as start_month,
                          last_day(my_month) as end_month,
                          FROM `kyosk-prod.karuru_scheduled_queries.monthly_date_variabes` 
                          where my_month >= '2023-12-01'
                          ),
---------------- DNs -----------------------
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                where date(created_at) > '2023-08-05'
                and is_pre_karuru = false
                ),
dns_report as (
              select distinct 
              country_code,
              territory_id,
              code as delivery_note,
              payment_request_id,
              outlet_id,
              --outlet.phone_number as outlet_phone_number,
              --outlet.name as outlet_name,
              --outlet.outlet_code as outlet_code,
              from karuru_dns
              where index = 1
              ),
dns_with_for_fraud_credit as (
                              SELECT distinct code, 
                              comment 
                              FROM `kyosk-prod.karuru_upload_tables.dns_for_fraud_credit`
                              ),
credit_disbursements as (
                          SELECT *, 
                          row_number()over(partition by id order by bq_upload_time desc) as index
                          FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                          WHERE date(disbursement_date) > '2023-08-01'
                          --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 4 month)
                          ),
credit_disbursement_report as (
                                select distinct id,
                                date(disbursement_date) as disbursement_date,
                                disbursement_date as disbursement_datetime,
                                date_trunc(date(disbursement_date), month) as disbursement_month,
                                date(due_date) as due_date,
                                date_trunc(date(due_date), month) as due_month,
                                payment_request_id,
                                credit_amount,
                                --service_fee_amount,
                                (credit_amount + service_fee_amount) as total_credit_amount
                                from credit_disbursements
                                where index = 1
                                ),
credit_repayments as (
                      select distinct r.id,
                      r.credit_id,
                      r.amount,
                      last_value(r.audit.last_modified)over(partition by r.credit_id order by r.audit.last_modified asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as repayment_date
                      from credit_disbursements cd, unnest(repayments) r
                      where index = 1
                      ),
credit_repayments_report as (
                            select distinct credit_id, 
                            date_trunc(date(repayment_date), month) as repayment_month,
                            date(repayment_date) as repayment_date,
                            sum(amount) as amount_repaid
                            from credit_repayments
                            group by 1,2,3
                            ),
credit_disbursements_and_repayments_report as (
                                                select cdr.*,
                                                coalesce(crr.amount_repaid,0) as amount_repaid,
                                                total_credit_amount - coalesce(crr.amount_repaid,0) as outstanding_amount,
                                                crr.repayment_date,
                                                crr.repayment_month,
                                                case
                                                  when crr.repayment_month is null then date_trunc(current_date, month) else  crr.repayment_month
                                                end as latest_month
                                                from credit_disbursement_report cdr
                                                left join credit_repayments_report crr on cdr.id = crr.credit_id
                                                ),
credit_mashup_with_date_variables as (
                                      select cdarr.id,
                                      mdv.start_month,
                                      mdv.end_month,
                                      cdarr.latest_month,
                                      cdarr.disbursement_date,
                                      cdarr.disbursement_datetime,
                                      cdarr.disbursement_month,
                                      cdarr.due_date,
                                      cdarr.due_month,
                                      date_diff(cdarr.due_date, mdv.end_month, day) as aging_in_days,
                                      case
                                        when cdarr.repayment_month <> start_month then null else repayment_date
                                      end as repayment_date,
                                      case
                                        when cdarr.repayment_month <> start_month then null else repayment_month
                                      end as repayment_month,
                                      cdarr.payment_request_id,
                                      cdarr.credit_amount,
                                      cdarr.total_credit_amount,
                                      case
                                        when cdarr.repayment_month <> start_month then 0 else cdarr.amount_repaid
                                      end as amount_repaid,
                                      from credit_disbursements_and_repayments_report cdarr,montly_date_variables mdv
                                      where (start_month >= cdarr.disbursement_month) and (start_month <= latest_month)
                                      ),
credit_with_outstanding_amount_report as (
                                            select cmwdv.*,
                                            (total_credit_amount - amount_repaid) as outstanding_amount
                                            from credit_mashup_with_date_variables cmwdv
                                            ),
credit_status_report as (
                          select cwoar.*,
                          case
                            when (comment is not null) and (outstanding_amount > 0) then 'FRAUD'
                            when (comment is not null) and (outstanding_amount <= 0) then 'RECOVERED'
                            when (comment is null) and (outstanding_amount <= 0) then 'RE-PAID'
                            when (comment is null) and (outstanding_amount > 0) and (aging_in_days between 0 and 7) then 'ACTIVE'
                            when (comment is null) and (outstanding_amount > 0) and (aging_in_days between -30 and -1) then 'OVERDUE'
                            when (comment is null) and (outstanding_amount > 0) and (aging_in_days between -90 and -31 ) then 'NON-PERFORMING'
                            when (comment is null) and (outstanding_amount > 0) and (aging_in_days <= -91) then 'BAD-DEBT'
                          else null end as credit_status,
                          dn.country_code,
                          dn.territory_id,
                          dn.outlet_id,
                          dns_with_for_fraud_credit.comment,
                          DENSE_RANK()over(partition by country_code order by disbursement_datetime asc) as credit_id_rank,
                          row_number()over(partition by country_code, id order by id, start_month asc) as credit_id_rank_by_month,
                          row_number()over(partition by country_code, id order by id, start_month desc) as credit_id_rank_by_month_desc
                          from credit_with_outstanding_amount_report cwoar
                          left join dns_report dn on cwoar.payment_request_id = dn.payment_request_id
                          left join dns_with_for_fraud_credit on dn.delivery_note = dns_with_for_fraud_credit.code
                          ),
credit_performance_report as (
                        select csr.*,
                        case
                          when (credit_status = 'FRAUD') then 'FRAUD'
                          when (credit_status = 'RECOVERED') then 'RECOVERED'
                          when credit_status in ('ACTIVE', 'RE-PAID') then 'Non-Overdue'
                          when (credit_status = 'OVERDUE') and (aging_in_days between -6 and 0) then 'Overdue 0-7 Days'
                          when (credit_status = 'OVERDUE') and (aging_in_days between -30 and -7) then 'Overdue 7-30 Days'
                          when (credit_status = 'NON-PERFORMING') and (aging_in_days between -60 and -31) then 'Non-Performing 30-60 Days'
                          when (credit_status = 'NON-PERFORMING') and (aging_in_days between -90 and -61) then 'Non-Performing 60-90 Days'
                          when (credit_status = 'BAD-DEBT') and (aging_in_days between -120 and -91) then 'Bad Debt 90-120 Days'
                          when (credit_status = 'BAD-DEBT') and (aging_in_days <= -121) then 'Bad Debt >120 Days'
                        else null end as credit_performance,
                        case when credit_id_rank_by_month = 1 then credit_amount else 0 end as credit_id_disbursed_amount,
                        case when credit_id_rank_by_month_desc = 1 then outstanding_amount else 0 end as credit_id_oustanding_amount,
                        case when credit_id_rank_by_month = 1 then total_credit_amount else 0 end as credit_id_total_disbursed_amount,
                        case when credit_status  in ('NON-PERFORMING', 'BAD-DEBT') then credit_amount else 0 end as npl_credit_amount,
                        case when credit_id_rank_by_month = 2 and credit_status in ('NON-PERFORMING', 'BAD-DEBT') then credit_amount else 0 end as credit_id_npl_amount
                        from credit_status_report csr
                        where territory_id  not in ('Test KE Territory', 'Test UG Territory', 'Test TZ Territory') 
                        ),
credit_performance_with_cumulative as (
                                      select *,
                                      sum(credit_id_disbursed_amount)over(partition by country_code order by credit_id_rank asc) as cumulative_disbursed_amount
                                      from credit_performance_report
                                      ),
---------------------- Manage Territory Dashboard Access ------------------
dashboard_users as (
                    SELECT distinct user_email, 
                    territory as user_territory
                    FROM `kyosk-prod.karuru_upload_tables.dashboard_users` 
                    ),
dashboard_territories as (
                          SELECT  distinct territory_id, 
                          territory_lists as user_territory_list
                          FROM `kyosk-prod.karuru_upload_tables.dashboard_territories`, unnest(territory_lists) as territory_lists
                          ),
dashboard_users_with_territories as (
                                    select distinct du.user_email,
                                    du.user_territory,
                                    dt.user_territory_list
                                    from dashboard_users du 
                                    left join dashboard_territories dt on du.user_territory = dt.territory_id
                                    ),
credit_performance_with_user_access as (
                                        select cpr.*except(territory_id),
                                        trm.new_territory_id as territory_id,
                                        --duwt.user_email,
                                        --duwt.user_territory 
                                        from credit_performance_with_cumulative cpr
                                        --left join dashboard_users_with_territories duwt on cpr.territory_id = duwt.user_territory_list
                                        left join territory_region_mapping trm on trm.original_territory_id = cpr.territory_id
                                        )
select distinct country_code, start_month, sum(credit_id_disbursed_amount) as credit_id_disbursed_amount,
sum(amount_repaid) as amount_repaid,
sum(npl_credit_amount) as npl_credit_amount, sum(outstanding_amount) as outstanding_amount
from credit_performance_with_user_access
where country_code in ('KE')
--and FORMAT_DATE('%Y%m%d', start_month) between @DS_START_DATE and @DS_END_DATE
--and REGEXP_CONTAINS(user_email,@DS_USER_EMAIL)
--order by id, start_month
group by 1,2 
order by start_month