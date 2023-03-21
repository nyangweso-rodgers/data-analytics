----------------------- QA - DN with items --------------
with delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            --where workflow_state in ('DELIVERED', 'SUBMITTED', 'DISPATCHED', 'DELIVERING', 'CHANGE_REQUESTED')
                            where workflow_state in ('PAID', 'DELIVERED')
                            and posting_date between '2022-08-01' and '2023-02-28'
                            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            and territory in ('Athi River')
                            ),
delivery_note_with_items as(
                            select distinct dn.territory,
                            dn.customer,
                            dn.name as delivery_note,
                            date_trunc(dn.posting_date, month) as posting_month,
                            dn.posting_date,
                            dn.grand_total,
                            dn.duka_latitude,
                            dn.duka_longitude
                            from delivery_note_with_index dn, unnest(items) i  where index = 1
                            )
select * from delivery_note_with_items