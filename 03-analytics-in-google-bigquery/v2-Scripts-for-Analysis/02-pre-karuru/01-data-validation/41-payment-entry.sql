---------------------- ERPNext - Payment Entry --------------------------
with
payment_entry as (
                  SELECT *, row_number()over(partition by name order by modified desc) as index
                  FROM `kyosk-prod.erp_reports.payment_entry` 
                  --where payment_type = "Receive"
                  --and date(creation) = '2023-07-10'
                  where date(creation) between '2021-01-01' and '2024-03-31'
                  --and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                  --and posting_date between '2023-09-19' and '2023-09-24'
                  and name = "ACC-PAY-2022-1642575"
                  ),                            
payment_entry_references_cte as (
                                select distinct pe.creation,
                                pe.posting_date,
                                pe.reference_date,

                                pe.company,
                                pe.territory,

                                pe.party_type,
                                pe.party,
                                pe.party_name,

                                pe.driver,

                                pe.payment_channel,
                                pe.mode_of_payment,
                                pe.payment_method_sub_category,

                                pe.kyosk_sales_order,
                                pe.kyosk_sales_invoice,
                                pe.payment_type,
                                pe.paid_from,
                                pe.paid_to,

                                pe.name, 
                                pe.payment_reference,
                                pe.status,
                                --paid_amount, 
                                --r.name,
                                r.reference_name,
                                r.total_amount,
                                r.allocated_amount,
                                r.outstanding_amount,

                                --pe.remarks
                                from payment_entry pe, unnest(references) r 
                                where index = 1 
                                order by creation
                                ),
payment_entry_summary_cte as (
                              select distinct name
                              from payment_entry_references_cte
                              )

select *
--distinct payment_reference, sum(total_amount) as total_amount,  sum(allocated_amount) as allocated_amount, 
--max(creation) as max_creation, max(posting_date) as max_posting_date
from payment_entry_references_cte 
--where payment_reference like 'KYS%'
group by 1