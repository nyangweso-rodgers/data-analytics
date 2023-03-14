-------------------- QA - Purchase Receipts with Suppliers ------------------
with
purchase_receipt_with_index as (
                                SELECT *, 
                                row_number()over(partition by name order by modified desc) as index 
                                FROM `kyosk-prod.erp_reports.purchase_receipt` 
                                where buying_type = "Purchasing"
                                and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                                --and workflow_state in ('COMPLETED') 
                                --and docstatus <> 1
                                and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                                --and posting_date between '2022-02-01' and '2022-02-28'
                                ),
purchase_receipt_with_suppliers as (
                                select distinct pr.supplier, pr.supplier_name,pr.buying_type,
                                max(pr.posting_date) as last_posting_datec,
                                current_date as current_date,
                                date_diff(current_date, max(pr.posting_date), month) as months_since_last_posting_date
                                from purchase_receipt_with_index pr, unnest(items) pri where index = 1 group by 1,2,3
                                order by 1,2,3
                                )
 

select * from purchase_receipt_with_suppliers