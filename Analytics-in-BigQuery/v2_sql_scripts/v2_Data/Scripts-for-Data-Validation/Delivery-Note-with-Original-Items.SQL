----------------------- QA - DN with original items --------------
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
delivery_note_with_original_items as(
                                      select distinct dn.lr_no, 
                                      scheduled_delivery_date,
                                      dn.name,
                                      dn.workflow_state,
                                      odni.fulfilment_status,
                                      odni.item_code,
                                      odni.uom,
                                      avg(rate) as rate,
                                      sum(qty) as qty,
                                      sum(odni.amount) as amount,
                                      coalesce(sum(odni.base_amount), sum(qty) * sum(odni.amount))   as base_amount
                                      from delivery_note_with_index dn, unnest(original_items) odni  where index = 1
                                      group by 1,2,3,4,5,6,7
                                      )
select * from delivery_note_with_original_items