with
accounts_cte as (
        select *
        from (
                SELECT createdAt,
                --updatedAt,
                id,
                accountTypeId,
                customerId,
                accountRef,
                status,
                fullDepositDate,
                dispatchDate,
                assignmentDate,
                assignmentId,
                jsfDate,
                jsfId, 
                --installationId,
                --installationDate,
                firstInstallmentDate,
                salesAgents,
                row_number() OVER (partition by id ORDER BY updatedAt DESC) as rnk 
                FROM sunculture.accounts
                ) where rnk = 1
        )
select *   
from accounts_cte
where accountRef = '14511018'