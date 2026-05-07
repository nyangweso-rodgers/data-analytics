SELECT --*
distinct date(sale_date),
counT(*), count(distinct account_id) 
FROM marts.vw_sales 
where date(sale_date) >=  '2026-05-04'
group by 1
ORDER BY 1 desc