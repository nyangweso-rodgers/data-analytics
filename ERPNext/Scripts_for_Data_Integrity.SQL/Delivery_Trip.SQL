select
    distinct date(creation), count(distinct name)
from `tabDelivery Trip` dt
where date(creation) between '2022-10-01' and '2022-10-31'
group by 1
order by 1