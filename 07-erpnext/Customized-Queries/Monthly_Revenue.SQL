select
    extract(YEAR_MONTH from dn.posting_date) as posting_month,
    count(distinct dn.customer) as count_of_customers,
    count(distinct kyosk_sales_order) as count_of_sales_orders,
    #count(distinct dn.name) as count_of_delivery_notes,
    sum(amount) as revenue
from `tabDelivery Note` dn
left join `tabDelivery Note Item` dni on dn.name = dni.parent
where dn.workflow_state in ('PAID', 'DELIVERED')
group by 1,2 order by 1,2