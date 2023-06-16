----------------------- QA - DN with items --------------
with delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            --where workflow_state in ('DELIVERED', 'SUBMITTED', 'DISPATCHED', 'DELIVERING', 'CHANGE_REQUESTED')
                            and workflow_state in ('PAID', 'DELIVERED')
                            and posting_date between '2023-01-01' and '2023-03-31'
                            and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --and territory in ('Athi River')
                            --where name in ('DN-MTWM-BYSQ', 'DN-MTWM-ABBY')
                            --where posting_date = '2023-03-25'
                            ),
delivery_note_summary as(
                            select distinct territory,
                            count(distinct dn.customer) as count_of_customers,
                            count(distinct dn.name) as count_of_delivery_notes,
                            count(distinct dn.name) / count(distinct dn.customer) as delivery_frequency,
                            sum(grand_total) / count(distinct dn.name) as basket_value,
                            count(distinct (case when dn.created_on_app = "Duka App" then dn.name else null end)) / count(distinct dn.name) as kyosk_app_adoption
                            from delivery_note_with_index dn 
                            where index = 1
                            group by 1
                            order by 2 desc
                            )
select * from delivery_note_summary