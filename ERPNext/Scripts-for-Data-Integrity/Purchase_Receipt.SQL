select 
    count(distinct name)
from `tabPurchase Receipt`  pr
#left join  `tabPurchase Receipt Item` pri on pr.name = pri.parent
#where company = 'KYOSK DIGITAL SERVICES LIMITED (UG)'
where date(creation) between '2022-12-01' and '2023-02-28'