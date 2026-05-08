with
writeoffs_cte as (
	SELECT id, accountId, woDate, amount, 
	#`type`, 
	createdAt, createdBy, updatedAt, updatedBy, xeroCreditNote, netsuiteCreditNoteId, writeOffReason, note
	#ledgerEntryID
	FROM amtdb.writeoffs
	)
select #*
distinct writeOffReason, count(accountId) as account_id_count
from writeoffs_cte 
group by 1
ORDER  by 2 desc
#order by updatedAt desc