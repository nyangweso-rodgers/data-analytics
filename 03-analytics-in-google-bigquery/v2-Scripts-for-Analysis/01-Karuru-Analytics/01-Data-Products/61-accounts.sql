SELECT * FROM `kyosk-prod.karuru_reports.account` WHERE TIMESTAMP_TRUNC(creation, DAY) > TIMESTAMP("2022-10-07")
--and parent_account = '40000000 - SALES - KDKE'
--and name = '40000001 - Gross Sales - Third Parties - KDKE'
--and parent_account = 'Expenses - KDKE'
--and parent_account = '50000000 - COST OF SALES - KDKE'
and parent_account = '55100000 - FULLFILLMENT EXPENSES - WAREHOUSING - KDKE'
order by name