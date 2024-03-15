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
credit_disbursements_raw as (
                              SELECT *, 
                              row_number()over(partition by id order by bq_upload_time desc) as index
                              FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                              WHERE date(disbursement_date) > '2023-08-01'
                              --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 4 month)
                              ),
credit_disbursement_report as (
                                select distinct id,
                                date(disbursement_date) as disbursement_date,
                                date_trunc(date(disbursement_date), month) as disbursement_month,
                                date(due_date) as due_date,
                                date_trunc(date(due_date), month) as due_month,
                                payment_request_id,
                                credit_amount,
                                --service_fee_amount,
                                (credit_amount + service_fee_amount) as total_credit_amount
                                from credit_disbursements_raw
                                where index = 1
                                ),
credit_repayments_raw as (
                        select distinct r.id,
                        r.credit_id,
                        r.amount,
                        last_value(r.audit.last_modified)over(partition by r.credit_id order by r.audit.last_modified asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as repayment_date
                        from credit_disbursements_raw c, unnest(repayments) r
                        where index = 1
                        ),
credit_repayments_report as (
                            select distinct credit_id, 
                            date_trunc(date(repayment_date), month) as repayment_month,
                            date(repayment_date) as repayment_date,
                            sum(amount) as amount_repaid
                            from credit_repayments_raw
                            group by 1,2,3
                            ),
credit_mashup_report as (
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
                                      select cmr.id,
                                      mdv.start_month,
                                      mdv.end_month,
                                      cmr.latest_month,
                                      cmr.disbursement_date,
                                      cmr.disbursement_month,
                                      cmr.due_date,
                                      cmr.due_month,
                                      date_diff(cmr.due_date, mdv.end_month, day) as aging_in_days,
                                      case
                                        when cmr.repayment_month <> start_month then null else repayment_date
                                      end as repayment_date,
                                      case
                                        when cmr.repayment_month <> start_month then null else repayment_month
                                      end as repayment_month,
                                      cmr.payment_request_id,
                                      cmr.credit_amount,
                                      cmr.total_credit_amount,
                                      case
                                        when cmr.repayment_month <> start_month then 0 else cmr.amount_repaid
                                      end as amount_repaid
                                      from credit_mashup_report cmr,montly_date_variables mdv
                                      where (start_month >= cmr.disbursement_month) and (start_month <= latest_month)
                                      ),
credit_with_outstanding_amount_report as (
                                            select cmwdv.*,
                                            (total_credit_amount - amount_repaid) as outstanding_amount
                                            from credit_mashup_with_date_variables cmwdv
                                            ),
credit_status_report as (
                          select cwoar.*,
                          case
                            when (comment is not null) then 'FRAUD'
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
                          from credit_with_outstanding_amount_report cwoar
                          left join dns_report dn on cwoar.payment_request_id = dn.payment_request_id
                          left join dns_with_for_fraud_credit on dn.delivery_note = dns_with_for_fraud_credit.code
                          ),
credit_performance_report as (
                              select csr.*,
                              case
                                when (credit_status = 'FRAUD') then 'FRAUD'
                                when  credit_status in ('ACTIVE', 'RE-PAID') then 'Non-Overdue'
                                when (credit_status = 'OVERDUE') and (aging_in_days between -6 and 0) then 'Overdue 0-7 Days'
                                when (credit_status = 'OVERDUE') and (aging_in_days between -30 and -7) then 'Overdue 7-30 Days'
                                when (credit_status = 'NON-PERFORMING') and (aging_in_days between -60 and -31) then 'Non-Performing 30-60 Days'
                                when (credit_status = 'NON-PERFORMING') and (aging_in_days between -90 and -61) then 'Non-Performing 60-90 Days'
                                when (credit_status = 'BAD-DEBT') and (aging_in_days between -120 and -91) then 'Bad Debt 90-120 Days'
                                when (credit_status = 'BAD-DEBT') and (aging_in_days <= -121) then 'Bad Debt >120 Days'
                              else null end as credit_performance
                              from credit_status_report csr
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
credit_performance_report_with_user_access as (
                                              select cpr.*except(territory_id),
                                              trm.new_territory_id as territory_id,
                                              --duwt.user_email,
                                              --duwt.user_territory 
                                              from credit_performance_report cpr
                                              --left join dashboard_users_with_territories duwt on cpr.territory_id = duwt.user_territory_list
                                              left join territory_region_mapping trm on trm.original_territory_id = cpr.territory_id
                                              where cpr.territory_id  not in ('Test KE Territory', 'Test UG Territory', 'Test TZ Territory') 
                                              )
select * from credit_performance_report_with_user_access
--where id in ('0F9EEWS2MAPB7', '0F8DNC2D9FPKJ', '0F8CZJ6BXFPYY', '0EYQ0TXE1DYYZ', '0FA0CQQ8RAPA8', '0EQN960VQC7Y2')
--where id in ('0EH9NMHWWYJMA')
--where FORMAT_DATE('%Y%m%d', start_month) between @DS_START_DATE and @DS_END_DATE
--and REGEXP_CONTAINS(user_email,@DS_USER_EMAIL)
order by id, start_month