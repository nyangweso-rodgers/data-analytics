-------------------------------- Customer Credit ---------------
with
customer_credit as (
                      SELECT *, row_number()over(partition by ID order by  LAST_MODIFIED_DATE desc) as index 
                      FROM `kyosk-prod.erp_reports.etl_credit_service_customer_credit_prod` 
                      WHERE DATE(creation) <= "2023-03-14"
                      )
select count(*) from customer_credit where index =1
