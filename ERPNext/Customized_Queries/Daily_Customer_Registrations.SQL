select
     distinct company, count(distinct territory) as territory_count, count(distinct name) as count_of_customers
from `tabCustomer` c
where creation between %(from_date)s and %(to_date)s
group by 1 order by 1