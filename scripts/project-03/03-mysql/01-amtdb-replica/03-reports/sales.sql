with
sales_cte as (
				SELECT account_id, 
				sale_date
				FROM amtdb.sales
				),
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
				#accountBypass, 
				#accountNotes, 
				status, 
				accountBalance, 
				#fvreceivable, 
				jsfDate, 
				jsfId, 
				parentAccountId, 
				dispatchDate, 
				expectedStartDate, 
				firstInstallmentDate, 
				isRevenuePosted, 
				revenuePostedAt, 
				#manualDate, 
				#revenueReversalAt, 
				#externalId, 
				installationId, 
				installationDate, 
				depositPaymentId, 
				fullDepositDate, 
				#externalIdSource, 
				#createdAt, 
				#createdBy, 
				#old_created_by, 
				#updatedAt, 
				#updatedBy, 
				#old_updated_by, 
				salesAgents, 
				assignmentId, 
				assignmentDate, 
				netSuiteAccountId
				FROM amtdb.accounts
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
	paygIntrest, 
	upfrontPaygFees, 
	#vat, 
	#cashNonTaxable, 
	#cashTaxable, 
	#cashVatAmount, 
	totalCashIncVAT, 
	depositAmount, 
	#totalPvVal, 
	#oldPayPlanAmount, 
	installmentAmount, 
	totalNumberPayments, 
	initialLoanAmt, 
	cashEquivalentPriceInclVat, 
	cashEquivalentPriceVatComponent, 
	cashEquivalentPriceExVat, 
	totalPaygPriceInclVat, 
	totalPaygPriceVatComponent, 
	totalPaygPriceExVat, 
	financeComponentInclVat, 
	financeComponentVatComponent, 
	financeComponentExVat, 
	isRefurbPayplan, 
	agentCommissionApplicableAmount, 
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
	payGUpfrontFees, 
	minDepositAmt, 
	maxDepositAmt, 
	minNoPayments, 
	maxNoPayments, 
	minInstallmentAmt, 
	kitNo, 
	maxInstallmentAmt, 
	selfRegistrationEnabled, 
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy, 
	notInstallable, 
	#erpClassCode, 
	slangName
	FROM amtdb.products
	),
product_types_cte as (
	SELECT id, 
	#old_id, 
	productType
	#createdAt, 
	#createdBy, 
	#updatedAt, 
	#updatedBy
	FROM amtdb.product_types
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
	referralOption, 
	interests, 
	customerSource, 
	partnerId, 
	referredById, 
	#shareReferral, 
	#email, 
	referralName, 
	referralPhoneNumber, 
	creditCheck, 
	creditCheckId, 
	reactivate, 
	#referralNationalId, 
	#createdAt, 
	#createdBy, 
	#old_created_by, 
	#updatedAt, 
	#old_updated_by, 
	#unLeashedId, 
	#vatValue, 
	salesAgents, 
	netSuiteId, 
	#alternativePhoneNumber, 
	salesForceId, 
	productOfInterest
	FROM amtdb.customers
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
employees_cte as (
        SELECT id, 
        #old_id, 
        name, 
        supervisorId, 
        departmentId, 
        #departmentId_old, 
        #commissionPayplanId, 
        #commissionPayplanId_old, 
        #identificationNumber, 
        #gender, 
        #dob, 
        #email, 
        #slack, 
        #phoneNumber, 
        #preferLanguage, 
        #recommendationLetter, 
        #employeePic, 
        #employeeIdPic, 
        #employeeContract, 
        countryId, 
        #countryId_old, 
        status, 
        isCustomer, 
        roleId, 
        #roleId_old, 
        #endDate, 
        #createdBy, 
        #created_by_old, 
        #createdAt, 
        #updatedBy, 
        #updated_by_old, 
        #updatedAt, 
        primaryRoleId, 
        salesForceAgentId, 
        contractType
        FROM amtdb.employees
        ),
departments_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	name
	#hod, 
	#createdAt, 
	#updatedAt
	FROM amtdb.departments
	),
sales_report_cte as (
				select distinct date(a.firstInstallmentDate) as first_installment_date,
				date(a.fullDepositDate) as full_deposit_date,
				date(s.sale_date) as sale_date,
				DATE_FORMAT(date(s.sale_date), '%Y-%m-01') AS sale_month,
				date(a.dispatchDate) as dispatch_date,
				DATE_FORMAT(date(a.dispatchDate), '%Y-%m-01') AS dispatch_month,
				a.installationDate as installation_date,
				DATE_FORMAT(date(a.installationDate), '%Y-%m-01') AS installation_month,
				a.expectedStartDate as expected_start_date,
				#pp.expiryDate as payplan_expiry_date,
				cr.companyName as company_name,
				c.companyRegionId as company_region_id,
				cr.region,
				s.account_id,
				a.accountRef as account_ref,
				a.customerId as customer_id,
				#a.old_customer_id,
				#c.customerSource as customer_source,
				c.name as customer_name,
				c.location as customer_location,
				#c.location1 as customer_location1,
				#c.location2 as customer_location2, 
				c.customerTypeId as customer_type_id,
				a.accountTypeId as account_type_id,
				account_type_cte.accountType as account_type,
				a.status as account_status,
				ap.payplanId,
				pp.name as payplan_name,
				products_cte.productTypeId as product_type_id,
				pt.productType as product_type,
				pp.productId as product_id,
				products_cte.product,
				products_cte.isRefurb as is_refurb,
				ap.productQty as product_qty,
				pp.cashValue as cash_value,
				pp.depositAmount as deposit_amount,
				c.salesAgents as customer_sales_agent,
				a.salesAgents as accounts_sales_agents,
				e.name as sales_agent_name,
				e.departmentId as department_id,
				departments_cte.name as department_name,
				date(a.jsfDate) as jsf_date,
				DATE_FORMAT(date(a.jsfDate), '%Y-%m-01') AS jsf_month,
				a.jsfId as jsf_id,
				a.installationId as installation_id
				from sales_cte s
				left join accounts_cte a on s.account_id = a.id
				left join account_type_cte on a.accountTypeId = account_type_cte.id
				left join account_payplans_cte ap on s.account_id = ap.accountId
				left join payplans_cte pp on ap.payplanId = pp.id
				left join products_cte on pp.productId = products_cte.id
				left join product_types_cte pt on pr.productTypeId = pt.id
				left join customers_cte c on a.customerId = c.id
				left join company_regions_cte cr on c.companyRegionId = cr.id
				left join employees_cte e on a.salesAgents = e.id
				left join departments_cte on e.departmentId = departments_cte.id
				#where s.account_id = 2
				#where a.customerId in ('1172699', '34886164', '33145147)
				#where a.old_customer_id in ('1172699', '34886164', '33145147')
				#where a.accountRef in ('1172699', '34886164', '33145147')
				#where date(sale_date) between '2025-01-01' and '2025-01-31'
				),
daily_sales_agg_cte as (
	select distinct 
	first_installment_date,
	full_deposit_date,
	sale_date,
	sale_month,
	dispatch_month,
	case
		when dispatch_month < sale_month THEN 'Error: Dispatch Before Sale'
		when dispatch_month is null then 'No Dispatch'
		ELSE TIMESTAMPDIFF(MONTH, sale_month, dispatch_month)
	end dispatch_lag,
	region,
	#customer_location,
	customer_id,
	customer_name,
	account_id,
	account_ref,
	#product_type,
	product,
	account_type,
	account_status,
	jsf_date,
	jsf_month,
	case
		when jsf_month < dispatch_month then 'Error: JSF Before Dispatch'
		when jsf_month is null then 'No JSF'
		else TIMESTAMPDIFF(MONTH, dispatch_month, jsf_month)
	end as post_dispatch_lag,
	case
		when jsf_month < sale_month then 'Error: JSF Before Sale'
		when jsf_month is null then 'No JSF'
		else TIMESTAMPDIFF(MONTH, sale_month, jsf_month)
	end as total_lead_time,
	jsf_id,
	installation_date,
	installation_month,
	product_qty
	#sum(product_qty) as product_qty
	from sales_report_cte
	where customer_name not like  '%test%'
	and account_status not in ('Refunded')
	and region = 'kenya'
	and product not in ('AfterSale', 'Installation', 'TSR', "Transport / Shipping", 'Extra Items', 'Agronomy', 'Samsung Galaxy A11')
	and date(sale_date) between '2024-12-01' and '2024-12-31'
	#group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
),
product_sales_agg_cte as (
	select distinct 
	#first_installment_date,
	#full_deposit_date,
	#sale_date,
	sale_month,
	#customer_location,
	region,
	#customer_location,
	product,
	account_type,
	count(distinct product) as product_count,
	#count(distinct customer_id) as customer_id_count,
	#count(distinct account_id) as account_id_count,
	#count(distinct account_ref) as account_ref_count,
	sum(product_qty) as product_qty
	#count(distinct jsf_id) as jsf_count
	#sum(product_qty) as product_qty
	from daily_sales_agg_cte
	#and product_type in ('Pump', 'Irrigation Kit')
	#and product_type not in ('Service')
	group by 1,2,3,4
	order by sale_month desc, product, account_type #product_qty desc
)
select distinct account_type
from sales_report_cte 
#from daily_sales_agg_cte
#from product_sales_agg_cte
