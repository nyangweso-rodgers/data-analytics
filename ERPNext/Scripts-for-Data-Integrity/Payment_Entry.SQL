select
    #distinct posting_date, count(distinct name) as count_of_payment_entries
    distinct posting_date, name, paid_amount
from `tabPayment Entry` 
where  payment_type = 'Receive'
and posting_date in ('2022-10-12','2022-10-21', '2022-10-22', '2022-10-23')
#group by 1
order by 1