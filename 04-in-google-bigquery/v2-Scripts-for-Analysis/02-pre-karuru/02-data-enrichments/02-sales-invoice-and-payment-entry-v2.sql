------------------ pre-karuru and karuru payment entries ------------------
with
----------------- pre-karuru -----------------------
pre_karuru_payment_entry as (
                  SELECT *, row_number()over(partition by name order by modified desc) as index
                  FROM `kyosk-prod.erp_reports.payment_entry` 
                  --where payment_type = "Receive"
                  --and date(creation) = '2023-07-10'
                  where date(creation) between '2022-01-01' and '2024-05-31'
                  --where date(creation) between '2021-01-01' and '2024-03-31'
                  --and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                  --and posting_date between '2023-09-19' and '2023-09-24'
                  --and name = "ACC-PAY-2022-1642575"
                  ),
pre_karuru_payment_entry_cte as (
                                  select distinct pe.creation,
                                  pe.party,
                                  pe.name,
                                  pe.payment_type,
                                  pe.payment_reference,
                                  paid_amount,
                                  per.reference_name,
                                  left(payment_reference, 3) as check_payment_reference,
                                  pe.driver,
                                  from pre_karuru_payment_entry pe, unnest(references) per
                                  WHERE index = 1 
                                  --and  pe.name in ('ACC-PAY-2022-90544')
                                  ),
--------------------------- karuru ---------------------
karuru_payment_entry as (
                  SELECT *,
                  row_number()over(partition by id  order by modified desc) as index
                  FROM `kyosk-prod.karuru_reports.payment_entry` 
                  --WHERE date(creation) < "2024-01-01"
                  --WHERE date(creation) between "2022-01-01" and '2023-12-31'
                  where date(creation) between '2022-01-01' and '2024-05-31'
                  --where date(creation) between '2021-01-01' and '2024-03-31'
                  --and company_id = 'KYOSK DIGITAL SERVICES LTD (KE)'
                  --and payment_type = 'Receive'
                  --and name = 'ACC-PAY-2022-1642575'
                  ),
karuru_payment_entry_cte as (
                              select distinct pe.creation,
                              pe.party,
                              pe.name,
                              pe.payment_type,
                              pe.payment_reference,
                              paid_amount,
                              per.reference_name,
                              left(payment_reference, 3) as check_payment_reference,
                              --pe.driver
                              from karuru_payment_entry pe, unnest(references) per
                              where index = 1 
                              --and pe.name in ('ACC-PAY-2022-90544')
                              ),
------------------- pre-karuru sales invoices ------------------
pre_karuru_sales_invoice as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.sales_invoice` 
                            --where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
  							            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            where date(creation) between '2022-01-01' and '2023-12-31'
                            --and docstatus = 1
                            ),
pre_karuru_sales_invoice_cte as (
                                  SELECT distinct si.posting_date,
                                  si.territory,
                                  si.name as sales_invoice,
                                  si.po_no as sales_order,
                                  si.grand_total
                                  FROM pre_karuru_sales_invoice si 
                                  where index = 1
                                  --and name in ('ACC-SINV-2022-80340', 'ACC-SINV-2022-88695')
                                  ),
-------------------- mashup -------------------
pre_karuru_and_karuru_payment_entry_mashup_cte as (
                                                    select distinct name
                                                    --coalesce(pkpe.name, kpe.name) as name
                                                    from pre_karuru_payment_entry_cte pkpe
                                                    union all (select distinct name from karuru_payment_entry_cte)
                                                    --full outer join (select distinct name from karuru_payment_entry_cte) kpe on pkpe.name = kpe.name
                                                    ),
pre_karuru_sales_invoice_with_payment_entries_cte as (
                                                      select pksi.*,
                                                      coalesce(pkpe.payment_reference, kpe.payment_reference) as payment_reference,
                                                      coalesce(pkpe.check_payment_reference, kpe.check_payment_reference) as check_payment_reference,
                                                      coalesce(pkpe.paid_amount, kpe.paid_amount) as paid_amount,
                                                      coalesce(pkpe.party, kpe.party) as party,
                                                      pkpe.driver
                                                      from pre_karuru_sales_invoice_cte pksi
                                                      left join pre_karuru_payment_entry_cte pkpe on pksi.sales_invoice = pkpe.reference_name
                                                      left join karuru_payment_entry_cte kpe on pksi.sales_invoice = kpe.reference_name
                                                      )
------------------------ validation ---------------------------------
--/*
SELECT distinct string_field_2  
FROM `kyosk-prod.karuru_test.pre_karuru_payment_entry` 
where string_field_2 not in (select name from pre_karuru_and_karuru_payment_entry_mashup_cte)
--*/
/*
select *,
--distinct sales_invoice
row_number()over(partition by sales_invoice order by payment_reference) as reference_name_index  
from pre_karuru_sales_invoice_with_payment_entries_cte
--where payment_reference is null
where FORMAT_DATE('%Y%m%d', posting_date) between @DS_START_DATE and @DS_END_DATE
*/
--select distinct name from karuru_payment_entry_cte where name in ()