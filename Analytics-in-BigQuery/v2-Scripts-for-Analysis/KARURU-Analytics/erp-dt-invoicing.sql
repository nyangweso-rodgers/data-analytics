-------------- ERPNext ---------------
------------ DTs Invoicing --------------
with
erp_dts as (
            select *,
            row_number()over(partition by name order by modified desc) as index
            from `erp_reports.delivery_trip`
            where  workflow_state in ('COMPLETED')
            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
            and company in ('KYOSK DIGITAL SERVICES LTD (KE)')
            --and date(completed_time) >= '2023-02-01'
          ),
dts_report as (
              select distinct date(dt.creation) as creation,
              date(completed_time) as completed_time,
              dt.company,
              territory,
              dt.name,
              vehicle,
              driver,
              driver_name,
              ds.delivery_note
              from erp_dts dt, unnest(delivery_stops) as ds
              where index = 1
              --and vehicle = 'KTWC654C'
              --'KBW 417M-1.5'
              ),
erp_dns as (
            SELECT *, 
            row_number()over(partition by name order by modified desc) as index 
            FROM `kyosk-prod.erp_reports.delivery_note` 
            ),
dns_report as (
                select distinct date(creation) as dn_creation,
                name,
                workflow_state,
                grand_total
                from erp_dns dn 
                where index = 1
                ),
erp_dns_distance as (
                      SELECT distinct name,
                      delivery_zone
                      FROM `kyosk-prod.erp_scheduled_queries.erp_dns_distances` 
                      ),
vehicle_mapping as (
                    SELECT distinct  Vehicle,
                    Vehicle_Type,
                    Tonnage
                    FROM `kyosk-prod.uploaded_tables.upload_vehicle_mapping` 
                    ),
dt_and_dns_report as (
                      select dtr.*,
                      dnr.dn_creation,
                      dnr.workflow_state,
                      dnr.grand_total,
                      d.delivery_zone,
                      vm.Vehicle_Type,
                      vm.Tonnage
                      from dts_report dtr
                      left join dns_report dnr on dtr.delivery_note = dnr.name
                      left join erp_dns_distance d on dtr.delivery_note = d.name
                      left join vehicle_mapping vm on dtr.vehicle = vm.Vehicle
                      )
select * from dt_and_dns_report
where workflow_state in ('PAID', 'DELIVERED')
--order by name, creation