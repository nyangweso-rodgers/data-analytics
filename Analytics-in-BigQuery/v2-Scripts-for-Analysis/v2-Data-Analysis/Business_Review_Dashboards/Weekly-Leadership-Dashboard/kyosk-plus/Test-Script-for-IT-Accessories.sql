------------------------- Test Script - IT Accessories ------------------
with
fx_rate as (select * from `uploaded_tables.uploaded_table_fx_rate_conversion_v5`),
delivery_note_items as (
                          select distinct dn.posting_date,
                          dn.company,
                          fxr.fx_rate_dfn,
                          fxr.fx_rate,
                          count(distinct dn.name) as count_of_dns,
                          sum(amount) as amount
                          from `kyosk-prod.erp_scheduled_queries.paid_and_delivered_dns_items` dn, fx_rate fxr
                          where item_group in ('IT Accessories','IT Accessories.')
                          and dn.company = fxr.company
                          and dn.posting_date between fxr.start_date and fxr.end_date
                          and posting_date between '2023-05-15' and '2023-05-21'
                          group by 1,2,3,4
                          )
select * from delivery_note_items
order by 1,2,3