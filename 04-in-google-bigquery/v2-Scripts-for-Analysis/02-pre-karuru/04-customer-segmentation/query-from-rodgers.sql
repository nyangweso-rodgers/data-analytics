----------- ERPNext --------------
----------- Customer Segmentation Query -------------
------------ Created By - Rodgers Nyangweso ------------------------
with 
------------------------------- Section by Rodgers ------------------
erp_revenue as (
                SELECT distinct date_trunc(posting_date,quarter) as posting_quarter,
                posting_date,
                last_value(posting_date) 
                        over(partition by customer, date_trunc(posting_date,quarter) order by posting_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
                  as customer_last_posting_date,
                company,
                customer,
                name,
                grand_total,
                grand_total_in_usd
                FROM `kyosk-prod.erp_scheduled_queries.erp_dns_revenue` 
                where territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                ),
quarterly_agg as (
                  select distinct posting_quarter,
                  customer_last_posting_date,
                  last_day(date_add(posting_quarter, interval 2 month)) as quarter_last_date,
                  company,
                  customer,
                  count(distinct name) as count_of_dns,
                  sum(grand_total) as total_revenue,
                  round(sum(grand_total_in_usd)) as total_revenue_usd,
                  count(distinct posting_date) as days_delivered,
                  round(sum(grand_total) / count(distinct name)) as avg_basket_value,
                  sum(grand_total_in_usd) / count(distinct name) as avg_basket_value_in_usd
                  from erp_revenue
                  group by 1,2,3,4,5
                  ),
quartely_rfm as (
                  select *,
                  date_diff(quarter_last_date, customer_last_posting_date, day) as recency
                  from quarterly_agg
                  ),
rfm_with_tiers as (
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
                      /*
                      CASE
                        WHEN Avg_basket_size <= 3000 THEN 'Low'
                        WHEN Avg_basket_size <= 6000 THEN 'Medium'
                      ELSE 'High' END AS monetary_Tier
                      */
                      from quartely_rfm
                      ),
rfm_segmentation as (
                      select *,
                      case
                        ---- #1. low monetary and frequency 100000----
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Low' and total_revenue_usd <= 250 THEN 'New Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 1' and total_revenue_usd <= 250 THEN 'Hesitant Occasional'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 2' and total_revenue_usd <= 250 THEN 'Not convinced'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'High' and total_revenue_usd <= 250 THEN 'Failed Onboarding'

                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Low' and total_revenue_usd <= 400 THEN 'New Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 1' and total_revenue_usd <= 400 THEN 'Hesitant Occasional'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 2' and total_revenue_usd <= 400 THEN 'Not convinced'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'High' and total_revenue_usd <= 400 THEN 'Failed Onboarding' 

                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Low' and total_revenue_usd <= 200 THEN 'New Customers'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 1' and total_revenue_usd <= 200 THEN 'Hesitant Occadional'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 2' and total_revenue_usd <= 200 THEN 'Not convinced'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'High' and total_revenue_usd <= 200 THEN 'Failed Onboarding'
                        
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Low' and total_revenue <= 100000 THEN 'New Customers'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 1' and total_revenue <= 100000 THEN 'Hesitant Occasional'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 2' and total_revenue <= 100000 THEN 'Not convinced'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'High' and total_revenue <= 100000 THEN 'Failed Onboarding'
                        

                        --- #2. medium monetary and frequency 500000-----
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Low' and total_revenue_usd <= 1500 THEN 'Promising Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 1' and total_revenue_usd <= 1500 THEN 'Regular Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 2' and total_revenue_usd <= 1500 THEN 'Slipping Regular'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'High' and total_revenue_usd <= 1500 THEN 'Disappointed Medium Value'

                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Low' and total_revenue_usd <= 2000 THEN 'Promising Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 1' and total_revenue_usd <= 2000 THEN 'Regular Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 2' and total_revenue_usd <= 2000 THEN 'Slipping Regulat'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'High' and total_revenue_usd <= 2000 THEN 'Disappointed Medium Value'

                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Low' and total_revenue_usd <= 1000 THEN 'Promising Customers'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 1' and total_revenue_usd <= 1000 THEN 'Regular Customers'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 2' and total_revenue_usd <= 1000 THEN 'Slipping Regular'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'High' and total_revenue_usd <= 1000 THEN 'Disappointed Medium Value'

                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Low' and total_revenue <= 500000 THEN 'Promising Customers'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 1' and total_revenue <= 500000 THEN 'Regular Customers'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 2' and total_revenue <= 500000 THEN 'Slipping Regular' 
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'High' and total_revenue <= 500000 THEN 'Disappointed Medium Value'
                        
                        --- #3. medium monetary and frequency 1000000-----
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Low' and total_revenue_usd <= 3000 THEN 'Loyal Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 1' and total_revenue_usd <= 3000 THEN 'Potential Loyalist'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 2' and total_revenue_usd <= 3000 THEN 'Slipping Loyalist'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'High' and total_revenue_usd <= 3000 THEN 'Disappointed High Value'

                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Low' and total_revenue_usd <= 4000 THEN 'Loyal Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 1' and total_revenue_usd <= 4000 THEN 'Potential Loyalist'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 2' and total_revenue_usd <= 4000 THEN 'Slipping Loyalist'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'High' and total_revenue_usd <= 4000 THEN 'Disappointed High Value'

                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Low' and total_revenue_usd <= 2000 THEN 'Loyal Customers'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 1' and total_revenue_usd <= 2000 THEN 'Potential Loyalist'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 2' and total_revenue_usd <= 2000 THEN 'Slipping Loyalist'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'High' and total_revenue_usd <= 2000 THEN 'Disappointed High Value'

                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Low' and total_revenue <= 1000000 THEN 'Loyal Customers'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 1' and total_revenue <= 1000000 THEN 'Potential Loyalist'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 2' and total_revenue <= 1000000 THEN 'Slipping Loyalist'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'High' and total_revenue <= 1000000 THEN 'Disappointed High Value'
                        
                        --- #4. High monetary and frequency 1000000-----
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Low' and total_revenue_usd > 3000  THEN 'VIP Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 1' and total_revenue_usd > 3000  THEN 'Potential VIP'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'Medium 2' and total_revenue_usd > 3000  THEN 'Slipping VIP'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (UG)' and recency_Tier = 'High' and total_revenue_usd > 3000  THEN 'Disappointed VIP'

                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Low' and total_revenue_usd > 4000  THEN 'VIP Customers'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 1' and total_revenue_usd > 4000  THEN 'Potential VIP'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'Medium 2' and total_revenue_usd > 4000  THEN 'Slipping VIP'
                        when company = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' and recency_Tier = 'High' and total_revenue_usd > 4000  THEN 'Disappointed VIP'
                        
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Low' and total_revenue_usd > 2000  THEN 'VIP Customers'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 1' and total_revenue_usd > 2000  THEN 'Potential VIP'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'Medium 2' and total_revenue_usd > 2000  THEN 'Slipping VIP'
                        when company = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED' and recency_Tier = 'High' and total_revenue_usd > 2000  THEN 'Disappointed VIP'

                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Low' and total_revenue > 1000000  THEN 'VIP Customers'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 1' and total_revenue > 1000000  THEN 'Potential VIP'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'Medium 2' and total_revenue >1000000  THEN 'Slipping VIP'
                        when company = 'KYOSK DIGITAL SERVICES LTD (KE)' and recency_Tier = 'High' and total_revenue > 1000000  THEN 'Disappointed VIP'
                      else null end as segment
                      from rfm_with_tiers
                      )
select * from rfm_segmentation
--where customer in ('INBO-Teklas shop at Birikani 00001', 'RSUB-OLAM VENTURE00001', 'P4X7-Aminu store00001')
--order by customer, posting_quarter