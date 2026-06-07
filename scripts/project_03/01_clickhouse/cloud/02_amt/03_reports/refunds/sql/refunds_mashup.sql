WITH
--------------------- Refunds ----------------------------------
refunds_cte as (
    select *
    from (
        SELECT distinct id,
        accountId,
        customer_id,
        paymentId,
        refundDate,
        refundCategoryId,
        refund_type,
        beneficiaryName,
        status,
        refundAmount,
        createdAt,
        row_number() OVER (PARTITION BY accountId ORDER BY updatedAt DESC) AS rnk
        FROM amt.refunds
        WHERE status = 'APPROVED'
        ) where rnk = 1
    ),
--------------------- Refund Categories ----------------------------------
refund_categories_cte as (
    SELECT distinct id,
    name,
    row_number() OVER (PARTITION BY id ORDER BY updatedAt DESC) AS rnk
    FROM amt.refund_categories
    ),
--------------------- Refunds Mashup ----------------------------------  
refunds_report_cte as (
    select distinct refunds_cte.id as refundId,
    refunds_cte.accountId as accountId,
    refunds_cte.customer_id as customerId,
    refund_categories_cte.name as refundCategory,
    refunds_cte.refundAmount as refundAmount,
    refunds_cte.refundDate as refundDate
    from refunds_cte
    left join refund_categories_cte on refund_categories_cte.id =  refunds_cte.refundCategoryId
)
select *
--from refunds_cte
from refunds_report_cte
LIMIT 31 OFFSET 0;