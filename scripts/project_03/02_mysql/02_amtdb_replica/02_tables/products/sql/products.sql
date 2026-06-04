with
products_cte as (
	SELECT id,
	#old_id, 
	productTypeId, 
	#old_product_type_id, 
	companyRegionId, 
	product, 
	mainProductId, 
	#old_main_product, 
	isRefurb, 
	price,  -- all null
	cashNonTaxable, 
	cashTaxable, 
	cashVatAmount, 
	totalCashIncVAT, 
	shortPayGInterest, 
	shortUpFrontFee, 
	lengthPayGInterest, 
	lengthUpFrontFee, 
	isActive, 
	isMain, 
	discountCodeId, 
	monthlyPaygIntrest, 
	tax, 
	payGUpfrontFees, 
	minDepositAmt, 
	maxDepositAmt, 
	minNoPayments, 
	maxNoPayments, 
	minInstallmentAmt, 
	kitNo, 
	maxInstallmentAmt, 
	selfRegistrationEnabled, 
	createdAt, 
	createdBy, 
	#updatedAt, 
	updatedBy, 
	notInstallable
	#erpClassCode, 
	#slangName -- all null
	FROM amtdb.products
	),
company_regions_cte as (
	SELECT id, 
	region, 
	companyName, 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	defaultCurrencyId
	FROM amtdb.company_regions
	),
products_mashup_cte as (
	select distinct 
	company_regions_cte.region,
	products_cte.product,
	notInstallable,
	isActive
	from products_cte
	left join company_regions_cte on company_regions_cte.id = products_cte.companyRegionId
	order by region
	)
select *
from products_cte