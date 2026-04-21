with
payplans_cte as (
	SELECT id, 
	#old_id, companyRegionId, 
	accountTypeId, 
	#old_account_type_id, 
	productId, 
	#old_product_id, subProductId, 
	name, 
	#cashValue, cashTaxAmt, totalCash, paygIntrest, upfrontPaygFees, vat, cashNonTaxable, cashTaxable, cashVatAmount, totalCashIncVAT, 
	depositAmount, totalPvVal, oldPayPlanAmount, installmentAmount, totalNumberPayments, initialLoanAmt, cashEquivalentPriceInclVat, cashEquivalentPriceVatComponent, cashEquivalentPriceExVat, 
	totalPaygPriceInclVat, totalPaygPriceVatComponent, totalPaygPriceExVat, 
	#financeComponentInclVat, financeComponentVatComponent, financeComponentExVat, 
	isRefurbPayplan,
	#agentCommissionApplicableAmount, isTAAllow, threshold, 
	isActive, channelUssd, expiryDate, taxableComponent, tax, nonTaxableComponent, isGlobalPayGInterest, isGlobalUpfrontPaygFees, 
	#isGlobalTax, customerId, 
	createdAt, createdBy,
	#old_created_by, updatedAt, updatedBy, old_updated_by, 
	isAddon, isUpgrade
	FROM amtdb.payplans
	)
SELECT #*
distinct name as payplanName, id as payplanId
from payplans_cte 
where name in ('CSD+RM2S.3,499.2,659.24months')
limit 100