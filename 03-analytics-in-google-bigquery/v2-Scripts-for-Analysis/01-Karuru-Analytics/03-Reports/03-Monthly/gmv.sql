with
delivery_notes as (
                select *,
                row_number()over(partition by id order by updated_at desc ) as index
                from `karuru_reports.delivery_notes` 
                where date(created_at) >= date_sub(date_trunc(current_date(), month), interval 3 month)
                --where date(created_at) >= '2024-01-01'
                and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
              ),
monthly_gmv as (
                    select distinct date_trunc(date(delivery_date),month) as delivery_month,
                    dn.country_code,
                    dn.territory_id,
                    sum(oi.total_delivered - (oi.qty_delivered * oi.discount_amount)) as amount_delivered
                    from delivery_notes dn,unnest(order_items) oi
                    where index = 1
                    and dn.status in ('PAID','DELIVERED','CASH_COLLECTED')
                    and oi.status = 'ITEM_FULFILLED'
                    group by 1,2,3
                  ),
credit_disbursements as (
                          SELECT *, 
                          row_number()over(partition by id order by bq_upload_time desc) as index
                          FROM `kyosk-prod.karuru_credit.credit_disbursements` 
                          WHERE date(disbursement_date) > '2023-08-01'
                          --where date(disbursement_date) > date_sub(date_trunc(current_date(), month), interval 4 month)
                          )
select *
from monthly_gmv
order by 2,1