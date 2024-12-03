------------------------------- Duplicated Sales Invoices per Payment Entry --------------------------
------------------------------ Created By: Rodgers ---------------------------------
with
sales_invoice_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
  							            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and date(creation) between '2022-01-01' and '2023-12-31'
                            --and name in ('SI-ATHI-WF96-23') -- 3 payment entries for testing
                            --and docstatus = 1
                            ),
payment_entry_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.payment_entry`
                            where date(creation) between '2022-01-01' and '2023-12-31'
                            ),
sales_invoice_with_items as (
                             SELECT distinct si.posting_date,
                             si.territory,
                             si.name as sales_invoice,
                             si.po_no as sales_order,
                             si.grand_total
                             FROM sales_invoice_with_index si where index = 1
                             ),
payment_entries as (
                      SELECT distinct --posting_date,
                      driver,
                      party,
                      --kyosk_sales_order as sales_order,
                      --pe.name as payment_entry,
                      payment_reference,
                      left(payment_reference, 3) as check_payment_reference,
                      paid_amount,
                      per.reference_name
                      FROM payment_entry_with_index pe, unnest(references) per
                      WHERE index = 1 
                      order by driver, party
                    ),
duplicated_sales_invoice_per_payment_entry as (
                                              select siwi.*, 
                                              driver,
                                              party,
                                              payment_reference,
                                              paid_amount,
                                              pe.check_payment_reference,
                                              from sales_invoice_with_items siwi
                                              --left join payment_entries pe on siwi.sales_order = pe.sales_order
                                              left join payment_entries pe on siwi.sales_invoice = pe.reference_name
                                              where FORMAT_DATE('%Y%m%d', siwi.posting_date) between @DS_START_DATE and @DS_END_DATE
                                              order by sales_invoice
                                              )
select *, 
row_number()over(partition by sales_invoice order by payment_reference) as reference_name_index  
from duplicated_sales_invoice_per_payment_entry