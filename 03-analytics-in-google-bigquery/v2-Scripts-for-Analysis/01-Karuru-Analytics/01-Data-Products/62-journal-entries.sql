with
journal_entry as (
                  SELECT * FROM `kyosk-prod.karuru_reports.journal_entry` 
                  WHERE date(creation) > "2021-01-01"
                  ),
journal_entry_list as (
                      select distinct je.creation,
                      je.modified,
                      posting_date,
                      company_id,
                      je.id,
                      je.name,
                      title,
                      voucher_type,
                      finance_book,
                      apply_tds,
                      total_debit,
                      total_credit,
                      difference,
                      multi_currency,
                      total_amount,
                      write_off_based_on,
                      write_off_amount,
                      is_opening,
                      a.name as account_name,
                      a.account,
                      a.account_type,
                      a.balance,
                      a.party_balance,
                      a.cost_center,
                      a.account_currency,
                      a.debit_in_account_currency,
                      a.debit,
                      a.credit_in_account_currency,
                      a.credit,
                      a.reference_type,
                      a.reference_name,
                      a.is_advance,
                      a.against_account,
                      remark,
                      pay_to_recd_from,
                      user_remark,
                      cheque_no,
                      voucher_type,
                      title,
                      a.owner,
                      a.modified_by,
                      a.party,
                      a.party_type
                      --min(creation)
                      from journal_entry je, unnest(accounts) a
                      )
select *
from journal_entry_list
--where remark is null
where id in ("ACC-JV-2024-KDNG-00050",
"ACC-JV-2024-KDTZ-00524",
"ACC-JV-2024-26193",
"ACC-JV-2024-26148",
"ACC-JV-2024-26144")