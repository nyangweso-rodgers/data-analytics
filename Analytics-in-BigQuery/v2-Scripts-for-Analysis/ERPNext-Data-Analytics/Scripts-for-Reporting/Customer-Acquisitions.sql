----------------------- DN - Customer Acquisitions --------------
with delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            and workflow_state in ('PAID', 'DELIVERED')
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and company in ('KYOSK DIGITAL SERVICES LIMITED (TZ)', 'KYOSK DIGITAL SERVICES LIMITED (UG)')
                            ),
monthly_summary as (
                      select distinct date_trunc(posting_date, month) as posting_month,
                      customer,
                      company,
                      sum(grand_total) as grand_total
                      from delivery_note_with_index dn where index = 1
                      group by 1,2,3
                      ),
customers_acquisition as (
                            select distinct company, customer, min(posting_date) as first_posting_date, date_trunc(min(posting_date), month) as first_posting_month
                            from delivery_note_with_index dn where index = 1
                            group by 1,2
                            order by 1,2
                            ),
august_acquisitions as (
                          select ca.*, 
                          ms.posting_month,
                          ms.grand_total
                          from customers_acquisition ca 
                          left join monthly_summary ms on ca.customer = ms.customer and ca.company = ms.company
                          where first_posting_month = '2022-08-01'
                          order by 1,2
                          )
select * from august_acquisitions