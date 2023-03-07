----------------------- QA - DN with items --------------
with delivery_note_with_index as (
                            SELECT *, 
                            row_number()over(partition by name order by modified desc) as index 
                            FROM `kyosk-prod.erp_reports.delivery_note` 
                            --where workflow_state in ('DELIVERED', 'SUBMITTED', 'DISPATCHED', 'DELIVERING', 'CHANGE_REQUESTED')
                            --where workflow_state in ('PAID', 'DELIVERED')
                            --and posting_date between '2023-01-01' and '2023-02-28'
                            --and posting_date in ('2023-02-09', '2023-02-14', '2023-02-20')
                        
                            --and date_trunc(posting_date, month) = '2023-01-01'
                            --and extract(hour from creation) <= 12
                            --and company = 'KYOSK DIGITAL SERVICES LTD (KE)'
                            --where lr_no = 'DT-ATHI-V2GI'
                            where name in ('DN-MAJM-B2OF')
                            ),
summary as (select distinct posting_date, count(distinct name) from delivery_note_with_index where index = 1 group by 1 order by 2 desc),

lists as (select distinct dn.name,  dn.grand_total from delivery_note_with_index dn where index = 1 order by 1,2),

delivery_note_with_original_items as(
                                      select distinct dn.territory,
                                      dn.lr_no, 
                                      dn.name,
                                      dn.workflow_state,
                                      i.item_code,
                                      i.uom,
                                      sum(i.amount) as amount,
                                      sum(i.base_amount) as base_amount
                                      from delivery_note_with_index dn, unnest(items) i  where index = 1
                                      group by 1,2,3,4,5,6
                                      )
select * from delivery_note_with_original_items