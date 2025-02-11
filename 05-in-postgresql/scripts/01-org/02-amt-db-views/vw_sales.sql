with
vw_sales_cte as (
                    SELECT 
                    customer_type, 
                    customer_type_id, 
                    identification_number, 
                    customer_names, 
                    phone_number, 
                    created_by, 
                    referral_option, 
                    created_at, 
                    KRA_pin, 
                    account_type, 
                    account_ref, 
                    product_units, 
                    product, 
                    amount, 
                    cumulative_amount, 
                    discount_amount, 
                    timestamp_made, 
                    payment_ref, 
                    gender, 
                    county_name, 
                    employee_id, 
                    employee_names, 
                    region, 
                    country_name, 
                    department_name, 
                    customer_id, 
                    location, 
                    longitude, 
                    latitude, 
                    gps, 
                    account_id, 
                    account_id_old, 
                    fullDepositDate, 
                    payment_id, 
                    is_active, 
                    status, 
                    payplan_name, 
                    downpayment_amount, 
                    installment_amount, 
                    payplan_id, 
                    total_number_payments, 
                    first_name, 
                    last_name
                FROM amtdb.vw_sales
                )
select * from vw_sales_cte
limit 2