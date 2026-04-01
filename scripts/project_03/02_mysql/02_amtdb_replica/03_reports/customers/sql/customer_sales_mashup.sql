with
customers_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	identificationNumber, 
	customerTypeId, 
	#old_customer_type_id, name, 
	phoneNumber, 
	#gender, location, location1, location2, latitude, longitude, referralOption, interests, customerSource, eventId, partnerId, referredById, shareReferral, email, 
	#referralName, referralPhoneNumber, creditCheck, creditCheckId, reactivate, referralNationalId, xeroContactId, 
	createdAt, 
	#createdBy, old_created_by, updatedAt, updatedBy, old_updated_by, unLeashedId, vatValue, 
	#salesAgents, 
	netSuiteId, 
	#revenuePin, taxExemptionCertNo, Phone_Number, 
	#identificationSerialNumber, documentType, kycDate, dateOfBirth, alternativePhoneNumber, meta, nationalIdFrontPic, nationalIdBackPic, 
	#salesForceId, productOfInterest, 
	isMigrated, 
	#ruleId, sundeskId, 
	walletID
	#optOutOfMessaging
	FROM amtdb.customers
	),
company_regions_cte as (
	SELECT id, 
	region
	#companyName,createdAt, createdBy, updatedAt, updatedBy, defaultCurrencyId
	FROM amtdb.company_regions
	),
customer_types_cte as (
	SELECT id, 
	#old_id, companyRegionId, saleType, 
	customerType 
	#createdAt, createdBy, updatedAt, updatedBy
	FROM amtdb.customer_types
	),
accounts_cte as (
	SELECT id, 
	#old_id, 
	accountTypeId, 
	#old_payplan, old_account_type_id, 
	customerId, 
	#old_customer_id, paygContractNumber, 
	accountRef, 
	#acreage, accountBypass, accountNotes, 
	status, accountBalance, 
	#fvreceivable, jsfDate, jsfId, parentAccountId, dispatchDate, expectedStartDate, firstInstallmentDate, 
	#isRevenuePosted, revenuePostedAt, manualDate, revenueReversalAt, externalId, installationId, installationDate, 
	depositPaymentId, fullDepositDate, 
	#externalIdSource, 
	createdAt, 
	#createdBy, old_created_by, updatedAt, updatedBy, old_updated_by, 
	salesAgents, 
	#assignmentId, assignmentDate, 
	netSuiteAccountId, 
	#isMigrated, isWalletActive, 
	walletID
	FROM amtdb.accounts
	),
sales_cte as (
			SELECT account_id, 
			sale_date
			FROM amtdb.sales
			),
account_type_cte as (
	SELECT id, 
	#old_id, 
	accountType 
	#createdAt, createdBy, updatedAt, updatedBy
	FROM amtdb.account_types
	),
account_payplans_cte as (
						SELECT id, 
						accountId, 
						payplanId, 
						productQty
						#createdAt, createdBy, updatedAt, updatedBy
						from amtdb.account_payplans
						),
payplans_cte as (
	SELECT id, 
	#old_id, 
	companyRegionId, 
	accountTypeId, 
	#old_account_type_id, 
	productId, 
	#old_product_id, subProductId, 
	name, 
	cashValue, 
	#cashTaxAmt, 
	#totalCash, # All NULL
	#paygIntrest, upfrontPaygFees, vat, cashNonTaxable, cashTaxable, cashVatAmount, totalCashIncVAT, 
	depositAmount, 
	#totalPvVal, oldPayPlanAmount, 
	installmentAmount, 
	totalNumberPayments, 
	#initialLoanAmt, cashEquivalentPriceInclVat, cashEquivalentPriceVatComponent, cashEquivalentPriceExVat, 
	totalPaygPriceInclVat
	#totalPaygPriceVatComponent, totalPaygPriceExVat, financeComponentInclVat, 
	#financeComponentVatComponent, financeComponentExVat, isRefurbPayplan, agentCommissionApplicableAmount, 
	#isTAAllow, threshold, isActive, channelUssd, expiryDate, taxableComponent, tax, 
	#nonTaxableComponent, isGlobalPayGInterest, isGlobalUpfrontPaygFees, isGlobalTax, 
	#customerId, createdAt, createdBy, old_created_by, updatedAt, updatedBy, old_updated_by, isAddon, isUpgrade
	FROM amtdb.payplans
	),
products_cte as (
	SELECT id,
	#old_id, 
	productTypeId, 
	#old_product_type_id, companyRegionId, 
	product, 
	#mainProductId, old_main_product, 
	isRefurb, 
	#price,  -- all null
	#cashNonTaxable, cashTaxable, cashVatAmount, 
	#totalCashIncVAT, shortPayGInterest, shortUpFrontFee, lengthPayGInterest, lengthUpFrontFee, isActive, 
	#isMain, discountCodeId, monthlyPaygIntrest, tax, payGUpfrontFees, minDepositAmt, 
	#maxDepositAmt, minNoPayments, maxNoPayments, minInstallmentAmt, kitNo, 
	maxInstallmentAmt
	#selfRegistrationEnabled, createdAt, createdBy, 
	#updatedAt, updatedBy, 
	#notInstallable, erpClassCode, slangName
	FROM amtdb.products
	),
employees_cte as (
        SELECT id, 
        #old_id, 
        name,
        supervisorId, 
        departmentId 
        #departmentId_old, commissionPayplanId, commissionPayplanId_old, 
        #identificationNumber, 
        #gender, dob, email, slack, 
        #phoneNumber, 
        #preferLanguage, recommendationLetter, employeePic, employeeIdPic, 
        #employeeContract, countryId, countryId_old, 
        #status, 
        #isCustomer, roleId, roleId_old, 
        #endDate, 
        #createdBy, 
        #created_by_old, createdAt, updatedBy, updated_by_old, updatedAt, 
        #primaryRoleId, 
        #salesForceAgentId,contractType
        FROM amtdb.employees
        ),
customers_mashup_cte as (
	select distinct company_regions_cte.region,
	date(customers_cte.createdAt) as customer_created_date,
	date(accounts_cte.createdAt) as account_created_at,
	date(fullDepositDate) as full_deposit_date,
	CASE 
        WHEN (account_type_cte.accountType = 'ADDON' and accounts_cte.status not in  ('No Deposit')) THEN date(accounts_cte.createdAt)
    ELSE date(sales_cte.sale_date) END as sale_date,
    DATEDIFF(
    DATE(fullDepositDate),
    CASE
        WHEN account_type_cte.accountType = 'ADDON'
             AND accounts_cte.status NOT IN ('No Deposit')
        THEN DATE(accounts_cte.createdAt)
        ELSE DATE(sales_cte.sale_date)
    END
) AS days_diff,
	customers_cte.id as customerId,
	identificationNumber,
	phoneNumber as customer_phone_number,
	customer_types_cte.customerType as customer_type,
	accounts_cte.id as account_id,
	accountRef as account_ref,
	account_type_cte.accountType as account_type,
	accounts_cte.status as account_status,
	products_cte.product as product_name,
	account_payplans_cte.productQty as product_qty,
	#payplans_cte.name as payplan_name,
	((installmentAmount * totalNumberPayments) + depositAmount) as total_payplan_amount,
	payplans_cte.totalNumberPayments as total_number_payments,
	payplans_cte.cashValue as cash_value,
	payplans_cte.depositAmount as deposit_amount,
	payplans_cte.installmentAmount as installment_amount,
	customers_cte.isMigrated as customer_wallet_is_migrated,
	customers_cte.walletID as customer_wallet_id,
	accounts_cte.walletID as account_wallet_id,
	#netSuiteId as customer_netsuite_id,
	#netSuiteAccountId as netsuite_account_id,
	accounts_cte.salesAgents
	#employees_cte.name as sales_agent_name,
	#supervisors_cte.name as supervisor_name
	from customers_cte
	left join company_regions_cte on company_regions_cte.id = customers_cte.companyRegionId
	left join customer_types_cte on customer_types_cte.id = customers_cte.customerTypeId
	left join accounts_cte on accounts_cte.customerId = customers_cte.id
	left join account_type_cte on account_type_cte.id = accounts_cte.accountTypeId
	left join account_payplans_cte on account_payplans_cte.accountId = accounts_cte.id
	left join payplans_cte on payplans_cte.id = account_payplans_cte.payplanId
	left join products_cte on products_cte.id = payplans_cte.productId
	left join sales_cte on sales_cte.account_id = accounts_cte.id
	left join employees_cte on employees_cte.id = accounts_cte.salesAgents
	left join employees_cte as supervisors_cte on supervisors_cte.id = employees_cte.supervisorId
	#order by customer_created_at desc
	),
sales_report_cte as (
	select *
	from customers_mashup_cte
	where product_name NOT IN ('Transport / Shipping', 'TSR', 'TSR Uganda', 'Training', 'UG Extra Items', 'Extra Items', 'Installation', 'furrow', 'AfterSale', 'Agronomy', 'Samsung Galaxy A11') 
	and account_status not in ('No Deposit', 'No Deposit ', 'Refunded', 'On Hold', 'Partial Deposit', 'Rejected', 'Partial Refunded', 'Convert To Cash')
	and sale_date is not null
	#and sale_date between '2026-01-01' and '2026-01-14'
	and  region = 'kenya'
	),
sales_agg_report_cte as (
	select distinct region,
	sale_date,
	#product_name,
	count(distinct product_name) as product_count,
	count(distinct customerId) as customer_id_count,
	count(distinct account_id) as account_id_count,
	sum(product_qty) as product_qty
	from sales_report_cte
	group by 1,2
	order by 2 desc
	),
customers_agg_cte as (
	select distinct region,
	customer_created_date,
	customerId,
	#identificationNumber,
	#customer_phone_number,
	GROUP_CONCAT(DISTINCT account_type SEPARATOR '/') AS accountTypes,
	GROUP_CONCAT(DISTINCT account_status SEPARATOR '/') AS accountStatuses,
	count(distinct account_id) as accountIdcount,
	count(distinct product_name) as productCount,
	GROUP_CONCAT(DISTINCT product_name SEPARATOR '/') AS products
	from customers_mashup_cte
	where product_name NOT IN ('Transport / Shipping', 'TSR', 'TSR Uganda', 'Training', 'UG Extra Items', 'Extra Items', 'Installation', 'furrow', 'AfterSale', 'Agronomy', 'Samsung Galaxy A11') 
	and account_status not in ('No Deposit', 'No Deposit ', 'Refunded', 'On Hold', 'Partial Deposit', 'Rejected', 'Partial Refunded', 'Convert To Cash')
	#and  region = 'kenya'
	group by 1,2,3
	)
select *
#from customers_mashup_cte
#from customers_agg_cte
#from sales_report_cte
from sales_agg_report_cte
#where product_name NOT IN ('Transport / Shipping', 'TSR', 'TSR Uganda', 'Training', 'UG Extra Items', 'Extra Items', 'Installation', 'furrow', 'AfterSale', 'Agronomy', 'Samsung Galaxy A11')
#and account_status not in ('No Deposit', 'No Deposit ', 'Refunded', 'On Hold', 'Partial Deposit', 'Rejected', 'Partial Refunded', 'Convert To Cash')
#and region = 'kenya'
#where customerId = '48314'