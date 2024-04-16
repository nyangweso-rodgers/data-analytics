--------------------- Karuru ---------------
------------------ DNs Items  ------------------------
with
karuru_dns as (
                SELECT *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_notes` dn
                WHERE date(created_at) > '2022-01-01'
                ),
dns_report as (
                select distinct coalesce(date(dn.delivery_date), date(updated_at)) as delivery_date,
                dn.territory_id,
                dn.outlet_id
                from karuru_dns dn
                where index = 1
                and status in ('PAID','DELIVERED','CASH_COLLECTED')
                and territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory')
                ),
dns_outlets_list as (
                      select distinct outlet_id,
                      LAST_VALUE(territory_id) OVER (PARTITION BY outlet_id ORDER BY delivery_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS territory_id,
                      LAST_VALUE(delivery_date) OVER (PARTITION BY outlet_id ORDER BY delivery_date DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS delivery_date,
                      from dns_report 
                      ),
karuru_outlets as (
                  SELECT *,
                  row_number()over(partition by id order by updated_at desc) as index
                  FROM `kyosk-prod.karuru_reports.outlets` 
                  WHERE date(created_at) > '2022-01-01'
                  ),
karuru_outlets_lists as (
                        select date(created_at) as  created_at,
                        id,
                        market.market_name as market_name, 
                        from karuru_outlets
                        where index = 1
                        ),
outlets_mashup as (
                    select distinct coalesce(id, outlet_id) as id,
                    coalesce(market_name, territory_id) AS market_name,
                    coalesce(created_at, delivery_date) as created_at
                    from karuru_outlets_lists
                    full outer join dns_outlets_list on karuru_outlets_lists.id = dns_outlets_list.outlet_id
                    )
select * from outlets_mashup
where created_at is null