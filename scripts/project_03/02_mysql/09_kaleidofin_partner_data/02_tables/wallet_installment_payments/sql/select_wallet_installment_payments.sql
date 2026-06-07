with
wallet_installment_payments_cte as (
	SELECT id, accountId, instalmentScheduleId, ledgerEntryId, paymentType, amountPaid, amountRefunded, paymentDate, refundDate, paymentId, createdAt, updatedAt, _exported_at
	FROM kaleidofin_partner_data.wallet_installment_payments
	)
select 
count(*)
from wallet_installment_payments_cte