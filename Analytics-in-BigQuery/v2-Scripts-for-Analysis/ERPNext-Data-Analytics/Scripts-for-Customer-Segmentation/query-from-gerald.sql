----------- Customer Segmentation Query -------------
------------- Created By Gerald ----------------------
with 
delivery_note_with_index as (
                            SELECT *,
                            row_number()over(partition by name order by modified desc) as index
                            FROM `kyosk-prod.erp_reports.delivery_note`
                            where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                            and workflow_state in ('PAID', 'DELIVERED')
                            and posting_date between '2022-02-11' and '2023-03-31'
                            ),
------------------------------- Section by Rodgers ------------------
erp_quaterly_agg as (
                      SELECT distinct date_trunc(posting_date,quarter) as posting_quarter,
                      company,
                      customer,
                      territory,
                      count(distinct name) as count_of_dns,
                      sum(grand_total) as grand_total
                      FROM delivery_note_with_index
                      where index = 1
                      group by 1,2,3,4
                      ),
------------------------------- End ----------------
delivery_notes_tbl_with_wholesalers as (
                                        select customer, 
                                        posting_date, 
                                        name, 
                                        grand_total ,
                                        company, 
                                        territory, 
                                        items # remove 
                                        from delivery_note_with_index
                                        where index =1
                                        ),

---- Removing Wholesalers -----
duka_type as (
              select distinct customer, 
              duka_status 
              from kyosk-prod.uploaded_tables.customer_duka_type
              ),

duka_type_orders as (
                      select a.*, b.duka_status 
                      from delivery_notes_tbl_with_wholesalers a
                      left join duka_type b on a.customer = b.customer
                      ),

delivery_notes_tbl as (
                        select * 
                        from duka_type_orders
                        where duka_status = 'Retailer (duka)' or duka_status is Null
                        ),

total_deliveries as (
                      select customer, posting_date, count(distinct name) as total_deliveries 
                      from delivery_notes_tbl
                      group by 1,2
                      order by 2 desc
                      ),

ranking_dates as (
                  select *, 
                  '2023-03-31' as last_cal_day, 
                  row_number()over(partition by customer order by posting_date desc) as index 
                  from delivery_notes_tbl
                  ),

first_delivery as (
                    select *, 
                    row_number()over(partition by customer order by posting_date asc) as index 
                    from delivery_notes_tbl
                    ),

first_purchase_tbl as (
                        select customer, 
                        posting_date as first_purchase 
                        from first_delivery
                        where index= 1
                        ),

last_purchase_tbl as (
                      select customer,
                      company, 
                      posting_date as last_purchase, 
                      last_cal_day 
                      from ranking_dates
                      where index= 1
                      ),

recency_tbl as (
                select *, 
                date_diff(cast(last_cal_day as datetime), last_purchase, day) as recency 
                from last_purchase_tbl
                ),

frequency_tbl as (
                  select customer , 
                  count(distinct posting_date) as days_delivered, 
                  from delivery_notes_tbl
                  group by 1
                  ),

delivery_months_tbl as (
                        select *, 
                        date_trunc(posting_date, month) as month_date 
                        from delivery_notes_tbl
                        order by 2 desc
                        ),

total_del_rev_tbl as (
                      select customer, 
                      count(distinct name) as total_del, 
                      sum(grand_total) as total_revenue 
                      from delivery_months_tbl
                      group by 1
                      ),

monetary as (
              select *, 
              safe_divide(total_revenue, total_del) as Avg_basket_size 
              from total_del_rev_tbl
              group by 1,2,3
              ),

rfm as (
          select a.customer,
          a.company,* except(customer,company) 
          from recency_tbl a
          join frequency_tbl b on a.customer = b.customer
          join monetary c on a.customer = c.customer
          join first_purchase_tbl d on a.customer = d.customer 
          ),

----------------------------created on kyoskapp agent app------------------------------------------------------------------------
created_on as (
                select customer, 
                created_on_app, 
                date_trunc(posting_date, month) posting_month 
                from delivery_note_with_index
                order by posting_month desc
                ),

created_on_rank as (
                    select  *,
                    rank()over(partition by customer order by posting_month  desc) as index 
                    from created_on
                    ),

created_on_tbl as (
                    select * 
                    from created_on_rank
                    where index= 1
                    ),

------------------------------------------------------------------------------------------------------------------------------------
RFM_tbl as (
            select *,
            CASE
              WHEN recency <= 7 THEN 'Low'
              WHEN recency <= 30 THEN 'Medium 1'
              WHEN recency <= 90 THEN 'Medium 2'
              ELSE 'High' END AS recency_Tier,
            CASE
              WHEN days_delivered <= 1 THEN 'Low'
              WHEN days_delivered <=10 THEN 'Medium'
              ELSE 'High' END AS frequency_Tier,
              --     CASE
              --     WHEN Avg_basket_size <= 3000 THEN 'Low'
              --     WHEN Avg_basket_size <= 6000 THEN 'Medium'
              --     ELSE 'High'
              -- END AS monetary_Tier
              from rfm
              ),

---------------------------------------currency conversion and table  ---------------------------------------------------------------------

currency as (
              select company, 
              fx_rate 
              from `uploaded_tables.uploaded_table_fx_rate_conversion_v5`
              where fx_rate_dfn = 'Budget Rate FY 2024 (Apr. 2023 - March 2024)'
              ),

rfm_converted_tbl as (
                      select r.*, c.fx_rate, total_revenue/fx_rate as total_revenue_usd, Avg_basket_size/fx_rate as Avg_basket_size_usd,
                      from RFM_tbl r
                      left join currency c
                      on r.company = c.company
                      ),

---------------------------------------Segments table Uganda ------------------------------------------------------------------------------
segment_tbl_ug as (select * , CASE
    ----low monetary and frequency 100000----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 250 THEN 'New Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 250 THEN 'Hesitant Occasional'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 250 THEN 'Not convinced'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 250 THEN 'Failed Onboarding'

     ---medium monetary and frequency 500000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 1500 THEN 'Promising Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 1500 THEN 'Regular Customers'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 1500 THEN 'Slipping Regular'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 1500 THEN 'Disappointed Medium Value'

    ---medium monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 3000 THEN 'Loyal Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 3000 THEN 'Potential Loyalist'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 3000 THEN 'Slipping Loyalist'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 3000 THEN 'Disappointed High Value'
    -----High monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd > 3000  THEN 'VIP Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd > 3000  THEN 'Potential VIP'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd > 3000  THEN 'Slipping VIP'
    WHEN recency_Tier = 'High' and total_revenue_usd > 3000  THEN 'Disappointed VIP'
END AS Segment
from rfm_converted_tbl
Where company = 'KYOSK DIGITAL SERVICES LIMITED (UG)'
),


----------------------------------------Segment table Tanzania ---------------------------------------------------------------------------------

segment_tbl_tz as (select * , CASE
    ----low monetary and frequency 100000----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 400 THEN 'New Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 400 THEN 'Hesitant Occasional'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 400 THEN 'Not convinced'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 400 THEN 'Failed Onboarding'

     ---medium monetary and frequency 500000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 2000 THEN 'Promising Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 2000 THEN 'Regular Customers'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 2000 THEN 'Slipping Regulat'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 2000 THEN 'Disappointed Medium Value'

    ---medium monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 4000 THEN 'Loyal Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 4000 THEN 'Potential Loyalist'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 4000 THEN 'Slipping Loyalist'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 4000 THEN 'Disappointed High Value'
    -----High monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd > 4000  THEN 'VIP Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd > 4000  THEN 'Potential VIP'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd > 4000  THEN 'Slipping VIP'
    WHEN recency_Tier = 'High' and total_revenue_usd > 4000  THEN 'Disappointed VIP'
END AS Segment
from rfm_converted_tbl
Where company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
),

-----------------------------------Segment Nigeria ----------------------------------------------------------------------------------------

segment_tbl_ng as (select * , CASE
    ----low monetary and frequency 100000----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 200 THEN 'New Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 200 THEN 'Hesitant Occadional'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 200 THEN 'Not convinced'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 200 THEN 'Failed Onboarding'

     ---medium monetary and frequency 500000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 1000 THEN 'Promising Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 1000 THEN 'Regular Customers'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 1000 THEN 'Slipping Regular'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 1000 THEN 'Disappointed Medium Value'

    ---medium monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd <= 2000 THEN 'Loyal Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd <= 2000 THEN 'Potential Loyalist'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd <= 2000 THEN 'Slipping Loyalist'
    WHEN recency_Tier = 'High' and total_revenue_usd <= 2000 THEN 'Disappointed High Value'
    -----High monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue_usd > 2000  THEN 'VIP Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue_usd > 2000  THEN 'Potential VIP'
    WHEN recency_Tier = 'Medium 2' and total_revenue_usd > 2000  THEN 'Slipping VIP'
    WHEN recency_Tier = 'High' and total_revenue_usd > 2000  THEN 'Disappointed VIP'
END AS Segment
from rfm_converted_tbl
Where company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
),
----------------------------Segment Kenya ---------------------------------------------------------------------------------------------
segment_tbl_ke as (select * , CASE
      ----low monetary and frequency 100000----
    WHEN recency_Tier = 'Low' and total_revenue <= 100000 THEN 'New Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue <= 100000 THEN 'Hesitant Occasional'
    WHEN recency_Tier = 'Medium 2' and total_revenue <= 100000 THEN 'Not convinced'
    WHEN recency_Tier = 'High' and total_revenue <= 100000 THEN 'Failed Onboarding'

     ---medium monetary and frequency 500000-----
    WHEN recency_Tier = 'Low' and total_revenue <= 500000 THEN 'Promising Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue <= 500000 THEN 'Regular Customers'
    WHEN recency_Tier = 'Medium 2' and total_revenue <= 500000 THEN 'Slipping Regular'
    WHEN recency_Tier = 'High' and total_revenue <= 500000 THEN 'Disappointed Medium Value'

    ---medium monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue <= 1000000 THEN 'Loyal Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue <= 1000000 THEN 'Potential Loyalist'
    WHEN recency_Tier = 'Medium 2' and total_revenue <= 1000000 THEN 'Slipping Loyalist'
    WHEN recency_Tier = 'High' and total_revenue <= 1000000 THEN 'Disappointed High Value'
    -----High monetary and frequency 1000000-----
    WHEN recency_Tier = 'Low' and total_revenue > 1000000  THEN 'VIP Customers'
    WHEN recency_Tier = 'Medium 1' and total_revenue > 1000000  THEN 'Potential VIP'
    WHEN recency_Tier = 'Medium 2' and total_revenue >1000000  THEN 'Slipping VIP'
    WHEN recency_Tier = 'High' and total_revenue > 1000000  THEN 'Disappointed VIP'
END AS Segment
from rfm_converted_tbl
Where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
),
--------------------------Total counties -----------------------------------------------------------------------------------------------

total_countries as (select * from segment_tbl_ng
union all
select * from segment_tbl_tz
union all
select * from segment_tbl_ug
union all
select * from segment_tbl_ke),

--------------------------- Agent and Kyosk App -------------------------------------------------------------------

app_tbl as (select a.customer, a.company, a.Segment, b.created_on_app, b.posting_month from total_countries a
left join created_on_tbl b
on a.customer = b.customer),

--------------------------Territory----------------------------------------------------------------------------------------------
territory_name as (select customer, territory from delivery_note_with_index
group by 1, 2),


territory_tbl as (select  a.customer, a.company, a.Segment,a.total_revenue_usd, a.total_revenue, b.territory from total_countries a
left join territory_name b
on a.customer = b.customer),

--------------------------Reginal Mapping ----------------------------------------------------------------------------------------
regional_mapping as (select company, country, division, territory from kyosk-prod.erp_reports.upload_regional_mapping),

country_mappings as (select a.*, b.country, b.division from delivery_notes_tbl a
left join regional_mapping b 
on a.company = b.company and a.territory = b.territory 
),

mappings_final_tbl as (select a.customer, a.total_revenue,a.segment, b.country, b.division, b.territory from total_countries a
left join country_mappings b
on a.customer = b.customer
group by 1,2,3,4,5,6),

--------------------------SKU's----------------------------------------------------------------------------------------------------
item_groups as (select customer, items.item_group as item_group, amount from delivery_notes_tbl, UNNEST(items) as items),

sku_tbl as(select a.customer, a.company, a.segment, b.item_group, b.amount from total_countries a
left join item_groups b
on a.customer = b.customer),


----------------------Active age, Avg Daily order count, Avg Monthly order count, Account age -------------------------------------

active_age_tbl as (select *, date_diff(last_purchase, first_purchase, day) as active_age from total_countries),

daily_order_count as (select *, safe_divide(total_del, active_age) as avg_daily_order_count from active_age_tbl), ---Avg order count formular problem

monthly_order_count as (select *, (avg_daily_order_count * 30) as avg_monthly_order_count from daily_order_count),

final_tbl as (select *,(avg_monthly_order_count * Avg_basket_size_usd) as Avg_monthly_revenue_usd,
            (avg_monthly_order_count * Avg_basket_size) as Avg_monthly_revenue,
             date_diff(cast(last_cal_day as datetime), first_purchase, day) as account_age from monthly_order_count
             ),


final_with_division as (select a.*,b.division from final_tbl a
left join mappings_final_tbl b
on a.customer = b.customer),

-- select * from final_with_division
-- where customer = 'JJIB-Shangilia  shop Kikuyu Near  rubis  Petrol station 00001'
-- -- where customer  = 'HILAL WHOLESALE'
-- where Segment = 'Wholesalers' and company = 'KYOSK DIGITAL SERVICES LTD (KE)')

Avg_daily_order_count as (select *, safe_divide(total_rev, total_del) as Avg_basket_size_of_segment,
           safe_divide(total_del, active_age) as Avg_daily_order_count_seg,
           from (select Segment, count(distinct customer) as num_of_cust, avg(active_age) as active_age, sum(total_del) as total_del, sum(total_revenue) as total_rev,
           avg(account_age) as avg_account_age
           from final_with_division
-- where Segment = 'Wholesalers' 
where company = 'KYOSK DIGITAL SERVICES LTD (KE)'
-- where company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
-- where company =  'KYOSK DIGITAL SERVICES LIMITED (UG)' 
-- where company =  'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'                             
                                            
group by 1)
),

monthly_order_count_segment as (select *, (Avg_daily_order_count_seg * 30) as avg_monthly_order_count_seg  from Avg_daily_order_count),

final_segment_data_per_county as (select *, (Avg_basket_size_of_segment * Avg_monthly_order_count_per_cust) as avg_monthly_revenue_per_cust from (select *,
        (avg_monthly_order_count_seg * Avg_basket_size_of_segment) as Avg_monthly_revenue_seg,
        avg_monthly_order_count_seg/num_of_cust as Avg_monthly_order_count_per_cust,
        from monthly_order_count_segment)
),

--  SELECT  SUM(amount) AS total_amount
-- FROM sku_tbl
-- WHERE company = 'KYOSK DIGITAL SERVICES LTD (KE)' AND segment = 'Disappointed VIP'

Kenya_Sku_segmentation as (SELECT *
FROM (
  SELECT segment, item_group, SUM(amount) AS total_amount
  FROM sku_tbl
  WHERE company = 'KYOSK DIGITAL SERVICES LTD (KE)'
  -- AND segment = 'Disappointed VIP'
  GROUP BY segment, item_group
)
PIVOT (
  SUM(total_amount)
  FOR item_group IN ('Cooking Oil', 'White Sugar', 
'Wheat Flour',	
'Maize Flour',	
'Rice',
'Milk',
'Bar Soap',
'Energy Drink',
'Cooking Fat',	
'Juice',
'Diapers') -- specify your item groups here
)),

uganda_Sku_segmentation as (SELECT *
FROM (
  SELECT segment, item_group, SUM(amount) AS total_amount
  FROM sku_tbl
  WHERE company = 'KYOSK DIGITAL SERVICES LIMITED (UG)'
  -- AND segment = 'Disappointed VIP'
  GROUP BY segment, item_group
)
PIVOT (
  SUM(total_amount)
  FOR item_group IN ('All Purpose Flour', 'Cooking Oil', 'Bar Soap',
                                      'Brown Sugar','Wheat Flour','Soda','Grade-I Rice','Noodles',
                                      'Diapers','Aromatic Rice') -- specify your item groups here
)),

tanzania_Sku_segmentation as (SELECT *
FROM (
  SELECT segment, item_group, SUM(amount) AS total_amount
  FROM sku_tbl
  WHERE company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)'
  -- AND segment = 'Disappointed VIP'
  GROUP BY segment, item_group
)
PIVOT (
  SUM(total_amount)
  FOR item_group IN ('Brown Sugar', 'Cooking Oil','All Purpose Flour', 'Bar Soap',
                                      'Washing Powder','Spaghetti','Matches','Tooth Paste','Baking Powder',
                                      'Diapers','Bath Soap') -- specify your item groups here
)),

-- Selecting nigeria top products
--  SELECT item_group, SUM(amount) AS total_amount
--   FROM sku_tbl
--   WHERE company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
--   GROUP BY item_group
--   order by 2 desc
--   limit 10

nigeria_Sku_segmentation as (SELECT *
FROM (
  SELECT segment, item_group, SUM(amount) AS total_amount
  FROM sku_tbl
  WHERE company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED'
  -- AND segment = 'Disappointed VIP'
  GROUP BY segment, item_group
)
PIVOT (
  SUM(total_amount)
  FOR item_group IN ('Noodles','Spaghetti','Milk','Cooking Oil','Malt','Soda','Washing Powder','Spaghettini','Twist','Semovita') -- specify your item groups here
)),


-- select * from nigeria_Sku_segmentation


-----select customer details -------
delivery_notes_tbl_customer_details as (select distinct customer, customer_name, duka_latitude, duka_longitude, posting_date, contact_mobile, sales_partner,territory,
                                        ROW_NUMBER() OVER (PARTITION BY customer ORDER BY posting_date asc ) AS rn from delivery_note_with_index
where index =1),




customer_details as (select a.customer, b.customer_name, a.company, a.division, b.territory, a.segment, b.contact_mobile, b.sales_partner, b.duka_latitude, b.duka_longitude from final_with_division a
left join delivery_notes_tbl_customer_details b
on a.customer = b.customer
where b.rn =1 and contact_mobile is not Null and sales_partner is not Null
group by 1,2,3,4,5,6,7,8,9,10
)


select customer, 
          Avg_basket_size as Avg_basket_size_march, 
          days_delivered as march_frequency,
          segment as march_segment 
          from final_with_division
WHERE company = 'KYOSK DIGITAL SERVICES LTD (KE)'
