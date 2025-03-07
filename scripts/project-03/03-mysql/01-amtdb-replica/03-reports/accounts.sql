with
accounts_cte as (
	SELECT id, 
	#old_id, 
	accountTypeId, 
	#old_payplan, 
	#old_account_type_id, 
	customerId, 
	#old_customer_id, 
	paygContractNumber, 
	accountRef, 
	#acreage, 
	accountBypass, 
	accountNotes, 
	status, 
	accountBalance, 
	fvreceivable, 
	jsfDate, 
	jsfId, 
	parentAccountId, 
	dispatchDate, 
	expectedStartDate, 
	firstInstallmentDate, 
	isRevenuePosted, 
	revenuePostedAt, 
	manualDate, 
	revenueReversalAt, 
	#externalId, 
	installationId, 
	installationDate, 
	depositPaymentId, 
	fullDepositDate, 
	externalIdSource, 
	createdAt, 
	createdBy, 
	#old_created_by, 
	#updatedAt, 
	#updatedBy, 
	#old_updated_by, 
	salesAgents, 
	assignmentId, 
	assignmentDate, 
	netSuiteAccountId
	FROM amtdb.accounts
	#where jsfId = '285528d0-4e90-4b0b-a130-3127ed2f4597'
	),
account_type_cte as (
	SELECT id, 
	#old_id, 
	accountType 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy
	FROM amtdb.account_types
	),
account_payplans_cte as (
						SELECT id, 
						accountId, 
						payplanId, 
						productQty
						#createdAt, 
						#createdBy, 
						#updatedAt, 
						#updatedBy
						from amtdb.account_payplans
						),
payplans_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	accountTypeId, 
	#old_account_type_id, 
	productId, 
	#old_product_id, 
	subProductId, 
	name, 
	cashValue, 
	#cashTaxAmt, 
	#totalCash, 
	#paygIntrest, 
	#upfrontPaygFees, 
	#vat, 
	#cashNonTaxable, 
	#cashTaxable, 
	#cashVatAmount, 
	#totalCashIncVAT, 
	depositAmount, 
	#totalPvVal, 
	#oldPayPlanAmount, 
	#installmentAmount, 
	#totalNumberPayments, 
	#initialLoanAmt, 
	#cashEquivalentPriceInclVat, 
	#cashEquivalentPriceVatComponent, 
	#cashEquivalentPriceExVat, 
	#totalPaygPriceInclVat, 
	#totalPaygPriceVatComponent, 
	#totalPaygPriceExVat, 
	#financeComponentInclVat, 
	#financeComponentVatComponent, 
	#financeComponentExVat, 
	isRefurbPayplan, 
	#agentCommissionApplicableAmount, 
	#isTAAllow, 
	#threshold, 
	isActive, 
	#channelUssd, 
	#expiryDate, 
	#taxableComponent, 
	#tax, 
	#nonTaxableComponent, 
	#isGlobalPayGInterest, 
	#isGlobalUpfrontPaygFees, 
	#isGlobalTax, 
	customerId, 
	#createdAt, 
	#createdBy, 
	#old_created_by, 
	#updatedAt, 
	#updatedBy, 
	#old_updated_by, 
	isAddon, 
	isUpgrade
	FROM amtdb.payplans
	),
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
	#price,  -- all null
	#cashNonTaxable, 
	#cashTaxable, 
	#cashVatAmount, 
	#totalCashIncVAT, 
	#shortPayGInterest, 
	#shortUpFrontFee, 
	#lengthPayGInterest, 
	#lengthUpFrontFee, 
	isActive, 
	#isMain, 
	#discountCodeId, 
	#monthlyPaygIntrest, 
	#tax, 
	#payGUpfrontFees, 
	#minDepositAmt, 
	#maxDepositAmt, 
	#minNoPayments, 
	#maxNoPayments, 
	#minInstallmentAmt, 
	kitNo, 
	maxInstallmentAmt, 
	#selfRegistrationEnabled, 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	notInstallable, 
	#erpClassCode, 
	slangName
	FROM amtdb.products
	),
company_regions_cte as (
	SELECT id, 
	region, 
	companyName
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	#defaultCurrencyId
	FROM amtdb.company_regions
	),
customers_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	customerTypeId, 
	#old_customer_type_id, 
	name, 
	#phoneNumber, 
	#gender, 
	location, 
	#location1, 
	#location2, 
	#latitude, 
	#longitude, 
	#referralOption, 
	#interests, 
	customerSource
	#partnerId, 
	#referredById, 
	#shareReferral, 
	#email, 
	#referralName, 
	#referralPhoneNumber, 
	#creditCheck, 
	#creditCheckId, 
	#reactivate, 
	#referralNationalId, 
	#createdAt, 
	#createdBy, 
	#old_created_by, 
	#updatedAt, 
	#old_updated_by, 
	#unLeashedId, 
	#vatValue, 
	#salesAgents, 
	#netSuiteId, 
	#alternativePhoneNumber, 
	#salesForceId, 
	#productOfInterest
	FROM amtdb.customers
	),
sales_cte as (
				SELECT account_id, 
				sale_date
				FROM amtdb.sales
				),
accounts_report_cte as (
	select distinct date(accounts_cte.createdAt) as account_created_at_date,
	#DATE_FORMAT(date(accounts_cte.createdAt), '%Y-%m-01') AS accounts_created_at_month,
	date(accounts_cte.firstInstallmentDate) as first_installment_date,
	date(accounts_cte.fullDepositDate) as full_deposit_date,
	case
		when account_type_cte.accountType = 'ADDON' then date(accounts_cte.createdAt)
		else date(sales_cte.sale_date)
	end as sale_date,
	date(accounts_cte.dispatchDate) as dispatch_date,
	products_cte.companyRegionId as company_region_id,
	company_regions_cte.region,
	customers_cte.location as customer_location,
	#company_regions_cte.companyName as company_name,
	accounts_cte.customerId as customer_id,
	customers_cte.name as customer_name,
	accounts_cte.accountTypeId as account_type_id,
	account_type_cte.accountType as account_type,
	accounts_cte.id as account_id,
	accounts_cte.accountRef as account_ref,
	accounts_cte.status as account_status,
	account_payplans_cte.id as account_payplans_id,
	account_payplans_cte.payplanId as payplan_id,
	payplans_cte.name as payplan_name,
	payplans_cte.productId as product_id,
	products_cte.product as product,
	products_cte.isRefurb as is_refurb,
	account_payplans_cte.productQty as product_qty,
	payplans_cte.cashValue as cash_value,
	payplans_cte.depositAmount as deposit_amount,
	date(accounts_cte.assignmentDate) as assignment_date,
	accounts_cte.assignmentId as assignment_id,
	date(accounts_cte.jsfDate) as jsf_date,
	accounts_cte.jsfId as jsf_id,
	date(accounts_cte.installationDate) as installation_date,
	accounts_cte.installationId as installation_id,
	accounts_cte.salesAgents as accounts_sales_agent
	from accounts_cte
	left join account_type_cte on account_type_cte.id = accounts_cte.accountTypeId
	left join account_payplans_cte on account_payplans_cte.accountId = accounts_cte.id
	left join payplans_cte on payplans_cte.id = account_payplans_cte.payplanId
	left join products_cte on products_cte.id = payplans_cte.productId
	left join company_regions_cte on company_regions_cte.id = products_cte.companyRegionId
	left join customers_cte on customers_cte.id = accounts_cte.customerId
	left join sales_cte on sales_cte.account_id = accounts_cte.id
	#where accounts_cte.id = "133397"
	order by account_created_at_date desc
	),
sales_report_cte as (
	select distinct 
	account_created_at_date,
	full_deposit_date,
	sale_date,
	DATE_FORMAT(sale_date, '%Y-%m-01') AS sale_month,
	dispatch_date,
	DATE_FORMAT(dispatch_date, '%Y-%m-01') AS dispatch_month,
	case
		when dispatch_date < sale_date THEN 'Error: Dispatch Before Sale'
		when dispatch_date is null then 'No Dispatch'
		ELSE TIMESTAMPDIFF(MONTH, sale_date, dispatch_date)
	end dispatch_lag,
	region,
	#customer_location,
	customer_id,
	customer_name,
	account_type_id,
	account_type,
	account_status,
	account_id,
	account_ref,
	payplan_name,
	product_id,
	product,
	product_qty,
	#cash_value,
	deposit_amount,
	assignment_date,
	assignment_id,
	jsf_date,
	DATE_FORMAT(jsf_date, '%Y-%m-01') AS jsf_month,
	case
		when jsf_date < dispatch_date then 'Error: JSF Before Dispatch'
		when jsf_date is null then 'No JSF'
		else TIMESTAMPDIFF(MONTH, dispatch_date, jsf_date)
	end as post_dispatch_lag,
	jsf_id,
	installation_date,
	DATE_FORMAT(installation_date, '%Y-%m-01') AS installation_month,
	case
		when jsf_date < sale_date then 'Error: JSF Before Sale'
		when jsf_date is null then 'No JSF'
		else TIMESTAMPDIFF(MONTH, sale_date, jsf_date)
	end as total_lead_time,
	installation_id
	#accounts_sales_agent
	from accounts_report_cte
	where customer_name not like  '%test%'
	and product not in ('AfterSale', 'Installation', 'TSR', "Transport / Shipping", 'Extra Items', 'Agronomy', 'Samsung Galaxy A11')
	and account_status not in ('Refunded')
	and sale_date is not null
	and sale_date between '2024-12-01' and '2025-03-08'
	and region = 'kenya'
	),
sales_agg_cte as (
		select distinct 
		DATE_FORMAT(date(sale_date), '%Y-%m-01') AS sale_month,
		region,
		sum(product_qty) as product_qty
		from sales_report_cte
		group by 1,2
		order by sale_month desc
	),
accounts_agg_report_cte as (
	select distinct
	accounts_created_at_month,
	account_type,
	product,
	count(distinct customer_id) as customer_id_count,
	count(distinct account_id) as account_id_count,
	sum(product_qty) as product_qty
	#count(distinct jsf_id) as jsf_id_count,
	from accounts_report_cte
	where account_type = 'ADDON'
	and region = 'kenya'
	group by 1,2,3
	order by accounts_created_at_month desc
	)	
select *
#distinct account_id, count(distinct account_type) as account_type_count
#from accounts_report_cte
from sales_report_cte
#from sales_agg_cte
#from accounts_agg_report_cte