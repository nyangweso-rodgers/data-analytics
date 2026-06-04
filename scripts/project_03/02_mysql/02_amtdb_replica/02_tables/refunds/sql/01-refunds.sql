with
refunds_cte as (
	SELECT id, 
	accountId, 
	#old_account_id, 
	paymentId, 
	#old_payment_id, 
	refundAmount, 
	refundDate, 
	referenceId, 
	refundCategoryId, 
	trackingId, 
	#beneficiaryNationalId, 
	#beneficiaryPhoneNumber, 
	beneficiaryName, 
	fulfillmentCode, 
	isActive, 
	createdAt, 
	#createdBy, 
	#old_created_by, 
	#updatedAt, 
	#updatedBy, 
	#old_updated_by, 
	#xeroItemId, 
	#xeroOverpaymentId, 
	#xeroPaymentId, 
	paymentTypeId, 
	isPosted, 
	note, 
	netSuiteRefundId, 
	status, 
	approved_by, 
	reason_for_refund, 
	refund_type, 
	transportAmt, 
	approvalDate, 
	isFulfilled, 
	fulfillmentDate, 
	failedfulfillmentReason
	FROM amtdb.refunds
	),
monthly_refunds_agg_cte as (
	select distinct DATE_FORMAT(date(createdAt), '%Y-%m-01') as created_at_month,
	count(distinct id) as refunds_id_count
	from refunds_cte
	group by 1
	order by 1
	)
select count(distinct id)
#from monthly_refunds_agg_cte
from refunds_cte where DATE_FORMAT(date(createdAt), '%Y-%m-01') <= '2025-05-01'