--------------- ERP, Karuru ------------
---------------- DTs Migration -----------
with
erp_dts as (
            select *,
            row_number()over(partition by name order by modified desc) as index
            from `erp_reports.delivery_trip`
            where  workflow_state in ('COMPLETED')
            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
            --and company in ('KYOSK DIGITAL SERVICES LTD (KE)')
          ),
erp_dts_report as (
                    select distinct date(dt.creation) as creation,
                    --date(completed_time) as completed_time,
                    territory,
                    dt.name,
                    dt.workflow_state,
                    vehicle,
                    driver,
                    driver_name,
                    from erp_dts dt
                    where index = 1
                    and dt.name = "DT-KARA-FLGK"
                    ),
karuru_dts as (
                select *,
                row_number()over(partition by id order by updated_at desc) as index
                FROM `kyosk-prod.karuru_reports.delivery_trips` 
                where date(created_at) = '2023-10-17'
                and territory_id not in ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory')
                and is_pre_karuru = true
              ),
karuru_dts_report as (
                      select distinct created_at as created_at,
                      id,
                      code,
                      status,
                      --country_code,
                      territory_id,
                      driver.id as driver_id,
                      driver.code as driver_code,
                      driver.name as driver_name,
                      from karuru_dts
                      where index = 1
                      --and code = "DT-KARA-FLGK"
                    ),
er_karuru_dt_mashup as (
                        select e.name,
                        e.territory,
                        k.territory_id,
                        e.territory = k.territory_id as check_territory_id,
                        e.workflow_state, 
                        k.status,
                        e.workflow_state = k.status as check_status,
                        e.driver,
                        k.driver_code,
                        e.driver = k.driver_code as check_driver_code,
                        e.driver_name,
                        k.driver_name,
                        e.driver_name = k.driver_name as check_driver_name
                        from erp_dts_report e
                        left join karuru_dts_report k on e.name = k.code
                        )
select * from er_karuru_dt_mashup