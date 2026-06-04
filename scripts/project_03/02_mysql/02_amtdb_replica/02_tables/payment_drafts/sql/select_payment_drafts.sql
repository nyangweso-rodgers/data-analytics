with
payment_drafts_cte as (
SELECT id, accountId, currencyId, source, paymentTypeId, customerId, sourceAmountCurrency, forexRate, status, note, paymentId, paymentRef, payerNames, payerNumber, timeStampMade, amount, endorsedBy, createdBy, createdAt, updatedAt, updatedBy, deletedAt, receiptUrl
FROM amtdb.payment_drafts
)
select #*
min(timeStampMade), max(timeStampMade)
from payment_drafts_cte
#where timeStampMade  = '1966-11-20 18:16:15'
limit 1000