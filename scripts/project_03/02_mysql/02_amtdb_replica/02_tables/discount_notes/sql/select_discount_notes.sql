with
discount_notes_cte as (
	SELECT id, accountId, 
	discountId, 
	note, isAdjusted, 
	#isXeroPosted, appsheetId, 
	createdBy, 
	updatedBy, 
	reversedBy,
	#appsheet_id, netsuiteCreditNoteId, netsuiteCreditNoteDate, 
	ledgerEntryID, isReversed, reversedAmount, reversalLedgerEntryID, isReversalPosted,
	createdAt,
	updatedAt,
	reversalDate,
	amount,
	sum(amount)over(partition by accountId order by createdAt asc) as cum_amount
	FROM amtdb.discount_notes
	where isReversed = 0
	order by accountId, createdAt
	)
select #*
count(*), count(distinct accountId)
from discount_notes_cte
#where accountId in ('117039')
limit 1000