select
    distinct si.posting_date, si.territory, si.customer, si.name, si.docstatus, count(distinct per.parent) as count_of_payment_entries
from `tabSales Invoice` si
left join `tabPayment Entry Reference` per on si.name = per.reference_name
where si.company = "KYOSK DIGITAL SERVICES LTD (KE)"
group by 1,2,3,4,5
having count_of_payment_entries = 0
order by 1 desc