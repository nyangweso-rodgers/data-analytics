--------------------------------ERPNext - QA - Material Requests ---------------------
with
material_request_with_index as(
                                SELECT *, row_number()over(partition by name order by modified desc) as index
                                FROM `kyosk-prod.erp_reports.material_request`
                                where material_request_type = 'Purchase'
  								              --territory not in ("Kyosk TZ HQ", "Kampala","Uganda","DKasarani","Kyosk HQ", "Kenya")
                               and company =  'KYOSK DIGITAL SERVICES LTD (KE)'
                               --and set_warehouse = 'Eldoret Receiving Bay - KDKE'
                               and date(creation) between '2023-01-01' and '2023-06-28'
                              )
SELECT date(creation), name, material_request_type
 FROM material_request_with_index
 WHERE index = 1