SELECT  #distinct 
#id, 
distinct companyRegionId, count(distinct ip.accountId)
#distinct accountId
#old_account_id, instalmentScheduleId, old_instalment_schedule_id, paymentId, old_payment_id, paymentType, amtPaid, amtRefund, paidDate, createdAt, createdBy, updatedAt, updatedBy, ledgerEntryID, discountRefunds
FROM amtdb.installment_payments as ip
left join (select distinct id, customerId from amtdb.accounts) as acc on acc.id = ip.accountId 
left join (select distinct id, companyRegionId from amtdb.customers) as c on c.id = acc.customerId 
where ip.accountId not in (select distinct accountId from amtdb.wallet_installment_payments)
group by 1
order by 2 desc