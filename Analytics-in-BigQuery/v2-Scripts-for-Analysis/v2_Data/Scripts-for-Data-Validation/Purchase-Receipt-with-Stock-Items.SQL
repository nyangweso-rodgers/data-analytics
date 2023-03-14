-------------------- QA - Purchase Receipts ------------------
with
purchase_receipt_with_index as (
                                SELECT *, 
                                row_number()over(partition by name order by modified desc) as index 
                                FROM `kyosk-prod.erp_reports.purchase_receipt` 
                                where buying_type = "Purchasing"
                                and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                                and workflow_state in ('COMPLETED') 
                                --and name = 'MAT-PRE-2022-00036'
                                --and name = 'MAT-PR-RET-2023-00036'
                                --and name in ('MAT-PRE-2022-00036', 'MAT-PRE-2022-05238', 'MAT-PRE-2022-05711', 'MAT-PRE-2022-06738', 'MAT-PRE-2022-13367')
                                --and docstatus <> 1
                                and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                                --and posting_date between '2022-02-01' and '2022-02-28'
                                ),
stock_items as (
                  select distinct pr.company, 
                  --pri.item_group, 
                  pri.item_code,
                  max(pr.posting_date) as last_posting_date,
                  current_date as current_date,
                  date_diff(current_date, max(pr.posting_date), month ) as months_since_last_posting_date,
                  string_agg(distinct pr.supplier_name,"/" order by pr.supplier_name) as supplier_names
                  from purchase_receipt_with_index pr, unnest(items) pri where index = 1
                  group by 1,2
                  order by 2
                  )
 
select * from stock_items