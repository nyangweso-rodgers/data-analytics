----------------------- ERPNext - QA - DN with items --------------
with 
delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where workflow_state in ('PAID')
                            --territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya") 
                            and posting_date between '2023-08-01' and '2023-08-06'
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            ),
delivery_note_summary as(
                            select distinct name, grand_total
                            from delivery_note_with_index dn where index = 1
                            )
select *
from delivery_note_summary