------------------------------ Purchase Receipt ------------------------------
with
purchase_receipt_with_index as (
                                SELECT *, 
                                row_number()over(partition by name order by modified desc) as index 
                                FROM `kyosk-prod.erp_reports.purchase_receipt` 
                                where date(creation) between '2022-12-01' and '2023-02-28'
                                ),
purchase_receipt_summary as (
                              select count(distinct name) as name 
                              from purchase_receipt_with_index
                              where index = 1
                              )

select * from purchase_receipt_summary