----------------------- QA - DN with items --------------
with delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            --where workflow_state in ('DELIVERED', 'SUBMITTED', 'DISPATCHED', 'DELIVERING', 'CHANGE_REQUESTED')
                            --where workflow_state in ('PAID', 'DELIVERED')
                            --and posting_date between '2022-08-01' and '2023-02-28'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and territory in ('Athi River')
                            where name in ('DN-MTWM-BYSQ', 'DN-MTWM-ABBY')
                            ),
delivery_note_summary as(
                            select distinct dn.name,
                            dn.workflow_state 
                            from delivery_note_with_index dn --where index = 1
                            )
select * from delivery_note_summary