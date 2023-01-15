------------------------------ Purchase Receipt Report ------------------------------
with
purchase_receipt_with_index as (
                                SELECT *, 
                                row_number()over(partition by name order by modified desc) as index 
                                FROM `kyosk-prod.erp_reports.purchase_receipt` 
                                where buying_type = "Purchasing"
                                and company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
                                --and posting_date = '2022-11-15'
                                ),
purchase_receipt_summary as (
                              select distinct posting_date, count(distinct name) as name 
                              from purchase_receipt_with_index
                              where index = 1
                              group by 1 order by 1 desc
                              ),
purchase_receipt_lists as (
                            select distinct pr.posting_date,
                            pr.company,
                            pr.territory,
                            --pr.buying_type,
                            pr.name as purchase_receipt,
                            pri.purchase_order,
                            pri.material_request,
                            pr.workflow_state,
                            pr.supplier,
                            pr.transporter_name,
                            pri.brand,
                            pri.item_group,
                            pri.name,
                            pri.item_code,
                            pri.item_name,
                            pri.uom,
                            pri.qty,
                            pri.received_qty,
                            pri.rejected_qty,
                            pri.returned_qty,
                            pri.rate,
                            pri.base_rate,
                            pri.base_amount
                            from purchase_receipt_with_index pr, unnest(items) pri
                            where index = 1
                            order by pr.name
                            )

select* from purchase_receipt_lists 
where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE  