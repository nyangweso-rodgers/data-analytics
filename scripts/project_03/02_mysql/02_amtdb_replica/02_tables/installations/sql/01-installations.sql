with
installations_cte as (
	SELECT id, 
	#old_installation_id, 
	premiseId, 
	#old_premise_id, 
	accountId, 
	#old_account_id, 
	irrigationDesignSketch, 
	#pictureWaterSource, 
	waterSourceDepthMeters, 
	waterColumnHeightMeters, 
	pumpUsage, 
	waterRequirementLpd, 
	tankHeightMeters, 
	elevationDifferenceMeters, 
	distanceToWaterSourceMeters, 
	irrigationPrice, 
	rainmakerPrice, 
	householdPrice, 
	totalPrice, 
	note, 
	systemWeightKg, 
	systemSizeCbm, 
	transportationCosts, 
	volumetricWeight, 
	createdAt, 
	createdBy, 
	#old_created_by, 
	updatedAt, 
	updatedBy
	FROM amtdb.installations
	)
select 
distinct accountId, date(createdAt) as created_at_date, count(distinct id) as id_count
from installations_cte
group by 1,2
having id_count > 1
order by created_at_date desc