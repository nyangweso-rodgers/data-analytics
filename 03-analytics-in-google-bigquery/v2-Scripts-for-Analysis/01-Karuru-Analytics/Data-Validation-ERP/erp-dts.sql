-------------- ERPNext ---------------
------------ DTs --------------
with
erp_dts as (
            select *,
            row_number()over(partition by name order by modified desc) as index
            from `erp_reports.delivery_trip`
            where  workflow_state in ('COMPLETED')
            and territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
            and company in ('KYOSK DIGITAL SERVICES LTD (KE)')
          ),
dts_report as (
              select distinct date(dt.creation) as creation,
              date(completed_time) as completed_time,
              territory,
              dt.name,
              vehicle,
              driver,
              driver_name,
              ds.delivery_note
              from erp_dts dt, unnest(delivery_stops) as ds
              where index = 1
              and vehicle = 'KBW 417M-1.5'
              )
select 
*
from dts_report
order by name, creation