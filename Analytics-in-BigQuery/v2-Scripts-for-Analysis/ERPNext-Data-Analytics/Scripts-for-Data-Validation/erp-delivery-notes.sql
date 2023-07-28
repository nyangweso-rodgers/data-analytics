----------------------- ERPNext - QA - DN with items --------------
with 
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            and workflow_state in ('PAID')
                            and posting_date between '2023-07-01' and '2023-07-27'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
delivery_note_summary as(
                            select distinct name, grand_total
                            from delivery_note_with_index dn where index = 1
                            )
select *
from delivery_note_summary