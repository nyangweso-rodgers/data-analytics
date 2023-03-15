----------------------- Credit Allocated -------------
with
credit_allocated as (
                      SELECT *, row_number()over(partition by ID order by  LAST_MODIFIED_DATE desc) as index 
                      FROM `kyosk-prod.erp_reports.etl_credit_service_credit_allocated_prod` 
                      WHERE DATE(creation) <= "2023-03-14"
                      )
select count(*) from credit_allocated where index =1