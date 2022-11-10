------------------- Purchase Receipt ----------------------- 
with 
purchase_receipt_with_index as (
                                SELECT *, row_number()over(partition by name order by modified desc) as index
                                FROM `kyosk-prod.erp_reports.purchase_receipt` 
                                )
select max(creation) from purchase_receipt_with_index where index = 1 