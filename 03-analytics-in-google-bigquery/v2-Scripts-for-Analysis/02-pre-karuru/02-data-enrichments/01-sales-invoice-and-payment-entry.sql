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
                            --and docstatus = 1
                            --and name = 'SI-ATHI-00FD-22'
                            --and name = "SI-ATHI-02CN-23"
                            ),
payment_entry_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.payment_entry`
                            where date(creation) between '2022-01-01' and '2024-12-31'
                            ),
sales_invoice_with_items as (
                             SELECT distinct si.posting_date,
                             si.territory,
                             si.name as sales_invoice,
                             si.po_no as sales_order,
                             si.grand_total
                             FROM sales_invoice_with_index si where index = 1
                             ),
---------------------- karuru - payment entries -------------------------
payment_entries_with_references_cte as (
                      SELECT distinct --posting_date,
                      pe.name as payment_entry_name,
                      pe.kyosk_sales_invoice,
                      pe.driver,
                      pe.party,
                      --kyosk_sales_order as sales_order,
                      --pe.name as payment_entry,
                      pe.payment_reference,
                      left(pe.payment_reference, 3) as check_payment_reference,
                      pe.paid_amount,
                      per.reference_name
                      FROM payment_entry_with_index pe, unnest(references) per
                      WHERE index = 1 
                    ),
----------------------- karuru -payment entries -------------------
karuru_payment_entry as (
                      SELECT *,
                      row_number()over(partition by id  order by modified desc) as index
                      FROM `kyosk-prod.karuru_reports.payment_entry` 
                      WHERE date(creation) between "2022-03-01" and '2023-12-31'
                      ),
karuru_payment_entry_cte as (
                      select distinct 
                      pe.party,
                      pe.name as payment_entry_name,
                      pe.payment_reference,
                      left(pe.payment_reference, 3) as check_payment_reference,
                      pe.reference_no,
                      pe.paid_amount,

                      r.reference_name,
                      r.total_amount,
                      r.outstanding_amount,
                      r.allocated_amount,
                      from karuru_payment_entry pe, unnest(references) r
                      where index =1
                      ),
----------------------------- Mashup ----------------------------
duplicated_sales_invoice_per_payment_entry as (
                                              select siwi.*, 
                                              
                                              coalesce(pewr.driver,'UNSET') as driver,
                                              coalesce(pewr.party, kpe.party) as party,

                                              coalesce(pewr.payment_entry_name, kpe.payment_entry_name) as payment_entry_name,
                                              coalesce(pewr.reference_name, kpe.reference_name) as reference_name,
                                              coalesce(pewr.payment_reference, kpe.payment_reference) as payment_reference,

                                              coalesce(pewr.paid_amount, kpe.paid_amount) as paid_amount,
                                              coalesce(pewr.check_payment_reference, kpe.check_payment_reference) as check_payment_reference,
                                              from sales_invoice_with_items siwi
                                              --left join payment_entries pe on siwi.sales_order = pe.sales_order
                                              left join payment_entries_with_references_cte pewr on siwi.sales_invoice = pewr.reference_name
                                              left join karuru_payment_entry_cte kpe on siwi.sales_invoice = kpe.reference_name
                                              --where FORMAT_DATE('%Y%m%d', siwi.posting_date) between @DS_START_DATE and @DS_END_DATE
                                              order by sales_invoice
                                              ),
report_with_reference_name_index as (
                                      select *, 
                                      row_number()over(partition by sales_invoice order by payment_reference) as reference_name_index  
                                      from duplicated_sales_invoice_per_payment_entry
                                      ),
monthly_agg as (
                select distinct date_trunc(posting_date, month) as posting_month,
                sales_invoice
                from report_with_reference_name_index where payment_entry_name is null
                )
select distinct date_trunc(posting_date, month) as posting_month, count(distinct sales_invoice) as sales_invoice_count from report_with_reference_name_index where payment_entry_name is null group by 1 order by 1
--select * from payment_entries where kyosk_sales_invoice = 'SI-MTWM-SD9U-22' --= 'SI-ATHI-00FD-22'

--select * from monthly_agg where posting_month in ('2022-02-01', '2022-03-01', '2022-04-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01', '2023-06-01', '2023-07-01', '2023-08-01', '2023-09-01')
--select * from monthly_agg where posting_month = '2023-01-01'