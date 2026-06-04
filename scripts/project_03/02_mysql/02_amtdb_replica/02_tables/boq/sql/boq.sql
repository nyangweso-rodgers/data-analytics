with
boq_cte as (
	SELECT id, 
	accountId, 
	payplanId, 
	productId, 
	boqDate, 
	salesOrderId, 
	boqStatus, 
	boqType, 
	quantity, 
	unitPrice, 
	totalPrice, 
	#parentBoqId, # all NULL
	warehouseId, 
	warehouseName, 
	isActive, 
	inActiveReason, 
	createdBy, 
	#updatedBy, 
	createdAt, 
	#updatedAt, 
	destinationId, 
	destinationName, 
	scheduleId, 
	saleTypeId, 
	salesOrderApprovalDate, 
	salesOrderDeclineDate, 
	salesOrderNumber, 
	#pdfLink, 
	totalPriceLessTax, 
	creditNoteId, 
	creditNoteDate, 
	goodsReturnId, 
	goodsReturnDate, 
	paymentId, 
	ledgerEntryID
	FROM amtdb.boq
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
	notInstallable
	#erpClassCode, 
	#slangName -- all null
	FROM amtdb.products
	),
boq_mashup_cte as (
	select distinct boq_cte.id as boq_id,
	date(boq_cte.boqDate) as boq_date,
	date(boq_cte.salesOrderApprovalDate) as sales_order_approval_date,
	boq_cte.boqType as boq_type, 
	boq_cte.boqStatus as boq_status,
	boq_cte.accountId as account_id,
	products_cte.product,
	products_cte.isRefurb as is_refurb,
	boq_cte.warehouseName as warehouse_name,
	boq_cte.destinationName as destination_name
	from boq_cte
	left join products_cte on products_cte.id = boq_cte.productId 
	order by account_id 
	)
select *
#distinct boq_status
from boq_mashup_cte