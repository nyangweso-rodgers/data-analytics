CREATE VIEW marts.accounts_mv (
    `account_id` Int32,
    `accountRef` String,
    `accountType` String,
    `salesAgents` Nullable(Int32),
    `status` Nullable(String),
    `Repossession_type` String,
    `Repossession_created_at` DateTime64(3),
    `repossessions_reason` String,
    `account_balance` Nullable(Decimal(11, 2)),
    `payg_contract_number` Nullable(String),
    `dispatchDate` Nullable(DateTime64(3)),
    `salesOrderNumber` Nullable(String),
    `fullDepositDate` Nullable(Date32),
    `jsfDate` Nullable(Date32),
    `warranty_expiry_date` Nullable(DateTime),
    `parentAccountId` Nullable(Int32),
    `externalId` Nullable(String),
    `createdAt` DateTime64(3),
    `updatedAt` DateTime64(3),
    `netsuite_account_id` Nullable(String),
    `customerId` Int32,
    `companyRegion` String,
    `customerType` String,
    `customerTypeId` Nullable(Int32),
    `customer_name` String,
    `phoneNumber` Nullable(String),
    `identification_number` String,
    `gender` Nullable(String),
    `nationalIdFrontPic` String,
    `documentType` Nullable(String),
    `Next_of_Kin_Name__c` String,
    `Next_of_Kin_Phone_Number__c` String,
    `creditCheck` LowCardinality(Nullable(String)),
    `creditCheckCustomer` LowCardinality(Nullable(String)),
    `product` Nullable(String),
    `productQty` Nullable(Int32),
    `deviceId` Nullable(String),
    `productId` Nullable(Int32),
    `payplan_id` Int32,
    `downpayment_amount` Int32,
    `installment_amount` Int32,
    `installment_date` Nullable(Date32),
    `total_number_payments` Int32,
    `payplan_name` Nullable(String),
    `expected_payments_number` Nullable(Int64),
    `employee_id` Int32,
    `employee_name` String,
    `employee_createdAt` DateTime64(3),
    `employee_status` Nullable(String),
    `employee_gender` Nullable(String),
    `relationship_manager` String,
    `employee_phoneNumber` Nullable(String),
    `employee_identificationNumber` Nullable(String),
    `supervisor_id` Int32,
    `supervisor_name` String,
    `RSM` String,
    `Department_name` String,
    `Full_Deposit_Date` Nullable(DateTime('UTC')),
    `sale_date` Nullable(DateTime64(3)),
    `PaidAmount` Decimal(38, 6),
    `first_payment_date` Nullable(DateTime64(3)),
    `last_payment_date` Nullable(DateTime64(3)),
    `discountAmount` Nullable(Decimal(38, 2)),
    `refundAmount` Float64,
    `Total_Paid` Decimal(38, 6),
    `RefundDate` Nullable(DateTime64(3)),
    `leadId` String,
    `LeadSource` String,
    `referralType` LowCardinality(Nullable(String)),
    `Referral_ID__c` Nullable(String),
    `Referral_Name__c` Nullable(String),
    `Referral_Phone_Number__c` String,
    `greatest(coalesce(a.updatedAt, toDateTime('1970-01-01 00:00:00')), coalesce(ranked_sales.sale_date, toDateTime('1970-01-01 00:00:00')), toDateTime('1970-01-01 00:00:00'))` DateTime64(3),
    `last_modified` DateTime64(3),
    `repossession_amount` Nullable(Int64),
    `writeoffs_amount` Nullable(Int64),
    `customerWalletIsMigrated` Nullable(Int32),
    `cds1_date` Nullable(Date),
    `cds2_date` Nullable(Date)
) AS WITH leads_cte AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT leadId,
                mobilePhone,
                toString(phoneNumber) AS phoneNumber,
                idNumber,
                leadAmtCustomerId,
                leadSourceId,
                employeeReferralId,
                referralId,
                referralType,
                name,
                row_number() OVER (
                    PARTITION BY leadId
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                `sales-service`.leads
        )
    WHERE
        rnk = 1
),
leadsources_cte AS (
    SELECT
        *
    FROM
        (
            SELECT
                id,
                name,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                `sales-service`.leadsources
        )
    WHERE
        rnk = 1
),
kyc_cte AS (
    SELECT
        idNumber,
        gender,
        dob
    FROM
        (
            SELECT
                idNumber,
                gender,
                dob,
                row_number() OVER (
                    PARTITION BY idNumber
                    ORDER BY
                        coalesce(updatedAt, createdAt) DESC
                ) AS rn
            FROM
                `sales-service`.kyc_requests
        )
    WHERE
        rn = 1
),
referrals AS (
    SELECT
        DISTINCT l.leadId AS leadId,
        r.name AS referral_name,
        r.idNumber AS referral_identificationNumber,
        r.mobilePhone AS referral_phoneNumber
    FROM
        leads_cte AS l
        LEFT JOIN leads_cte AS r ON (l.referralId = r.idNumber)
        OR (l.referralId = r.phoneNumber)
        OR (l.referralId = r.leadId)
    WHERE
        l.referralId != ''
),
ranked_leads AS (
    SELECT
        DISTINCT leads_cte.leadId AS leadId,
        leadsources_cte.name AS LeadSource,
        leads_cte.idNumber AS ID_Number__c,
        leads_cte.referralType AS referralType,
        referrals.referral_name AS Referral_Name__c,
        referrals.referral_identificationNumber AS Referral_ID__c,
        referrals.referral_phoneNumber AS Referral_Phone_Number__c
    FROM
        leads_cte
        LEFT JOIN leadsources_cte ON leadsources_cte.id = leads_cte.leadSourceId
        LEFT JOIN referrals ON referrals.leadId = leads_cte.leadId
),
ranked_boq AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT accountId AS accountId,
                salesOrderApprovalDate AS salesOrderApprovalDate,
                salesOrderNumber AS salesOrderNumber,
                boqStatus,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.boq
            WHERE
                boqStatus = 'BILLED'
        ) AS boq
    WHERE
        (rnk = 1)
        AND (boq.salesOrderApprovalDate IS NOT NULL)
        AND (boq.salesOrderApprovalDate != '')
),
ranked_account_payplans AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT accountId AS accountId,
                payplanId AS payplanId,
                productQty AS productQty,
                createdAt AS createdAt,
                updatedAt AS updatedAt,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.account_payplans
        ) AS apl
    WHERE
        rnk = 1
),
ranked_account_device AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT accountId AS accountId,
                deviceId AS deviceId,
                createdAt AS createdAt,
                updatedAt AS updatedAt,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.account_devices
        ) AS dev
    WHERE
        rnk = 1
),
ranked_payplans AS (
    SELECT
        *
    FROM
        (
            SELECT
                id AS id,
                productId AS productId,
                name AS name,
                depositAmount AS downpayment_amount,
                installmentAmount AS installment_amount,
                totalNumberPayments AS total_number_payments,
                updatedAt AS updatedAt,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.payplans
        ) AS payp
    WHERE
        rnk = 1
),
ranked_products AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT id,
                product AS product,
                updatedAt AS updatedAt,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.products
        ) AS pr
    WHERE
        rnk = 1
),
ranked_accounts AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT id,
                accountRef AS accountRef,
                multiIf(
                    accountTypeId = 2,
                    'PAYG',
                    accountTypeId = 4,
                    'LEASE',
                    accountTypeId = 5,
                    'ADDON',
                    'CASH'
                ) AS accountType,
                customerId AS customerId,
                salesAgents AS salesAgents,
                status AS status,
                dispatchDate AS dispatchDate,
                fullDepositDate AS fullDepositDate,
                expectedStartDate,
                jsfDate AS jsfDate,
                createdAt AS createdAt,
                parentAccountId,
                accountBalance,
                externalId,
                creditCheck,
                paygContractNumber,
                updatedAt AS updatedAt,
                expectedStartDate AS installment_date,
                netSuiteAccountId,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.accounts
        )
    WHERE
        rnk = 1
),
ranked_departments AS (
    SELECT
        *
    FROM
        (
            SELECT
                id AS id,
                name AS name,
                updatedAt AS updatedAt,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.departments
        ) AS dep
    WHERE
        rnk = 1
),
ranked_employees AS (
    SELECT
        *
    FROM
        (
            SELECT
                id AS id,
                name AS name,
                phoneNumber AS phoneNumber,
                identificationNumber AS identificationNumber,
                departmentId AS departmentId,
                supervisorId AS supervisorId,
                createdAt AS createdAt,
                status AS status,
                gender AS gender,
                updatedAt AS updatedAt,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.employees
        ) AS em
    WHERE
        rnk = 1
),
ranked_supervisor AS (
    SELECT
        *
    FROM
        (
            SELECT
                id AS id,
                supervisorId AS supervisorId,
                name AS name,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.employees
        ) AS emp
    WHERE
        rnk = 1
),
ranked_rm AS (
    SELECT
        *
    FROM
        (
            SELECT
                id AS id,
                name AS name,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.employees
        ) AS emp
    WHERE
        rnk = 1
),
ranked_view AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT customerId,
                employeeId,
                employees.name,
                row_number() OVER (
                    PARTITION BY customerId
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.views
                INNER JOIN amt.employees ON views.employeeId = employees.id
            WHERE
                views.viewTypeId IN (6, 21)
        ) AS vi
    WHERE
        rnk = 1
),
ranked_customers AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT id,
                multiIf(
                    companyRegionId = 1,
                    'kenya',
                    companyRegionId = 2,
                    'civ',
                    companyRegionId = 3,
                    'uganda',
                    'Global'
                ) AS companyRegion,
                name,
                phoneNumber,
                identificationNumber AS identification_number,
                gender AS gender,
                salesAgents AS salesAgents,
                creditCheck,
                nationalIdFrontPic AS nationalIdFrontPic,
                documentType AS documentType,
                customerTypeId AS customerTypeId,
                isMigrated,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.customers
        )
    WHERE
        rnk = 1
),
customer_types_cte AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT id,
                customerType,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.customer_types
        )
    WHERE
        rnk = 1
),
ranked_s3 AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT accountId,
                orientation,
                fileName,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                `sales-service`.s3_uploads
        )
    WHERE
        (rnk = 1)
        AND (orientation = 'front')
),
ranked_sales AS (
    SELECT
        DISTINCT account_id AS account_id,
        toTimeZone(toDateTime(sale_date), 'UTC') AS sale_date
    FROM
        marts.vw_sales
),
ranked_schedules AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT installment_schedules.accountId AS account_id,
                installment_schedules.expectedDate AS expected_date,
                installment_schedules.expectedDate - toIntervalMonth(1) AS installment_date,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.installment_schedules
            WHERE
                (installment_schedules.paymentSequence = 1)
                AND (installment_schedules.expectedDate IS NOT NULL)
        )
    WHERE
        rnk = 1
),
aggregated_refunds AS (
    SELECT
        *
    FROM
        (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        updated_At DESC
                ) AS rnk
            FROM
                (
                    SELECT
                        DISTINCT accountId AS accountId,
                        sum(toFloat64(refundAmount)) AS refundAmount,
                        max(approvalDate) AS RefundDate,
                        max(sync_at) AS updated_At
                    FROM
                        amt.refunds
                    WHERE
                        status = 'APPROVED'
                    GROUP BY
                        accountId
                ) AS agg
        )
    WHERE
        rnk = 1
),
aggregated_discounts AS (
    SELECT
        DISTINCT accountId,
        sum(amount) AS discountAmount
    FROM
        (
            SELECT
                DISTINCT accountId,
                discountId,
                amount,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.discount_notes
        )
    WHERE
        rnk = 1
    GROUP BY
        1
),
aggregated_payments AS (
    SELECT
        *
    FROM
        (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        first_payment_date DESC
                ) AS rnk
            FROM
                (
                    SELECT
                        account_id AS accountId,
                        sum(amount) AS PaidAmount,
                        min(timestamp_made) AS first_payment_date,
                        max(timestamp_made) AS last_payment_date
                    FROM
                        marts.vw_payments
                    GROUP BY
                        account_id
                ) AS agg
        )
    WHERE
        rnk = 1
),
aggregated_warranty AS (
    SELECT
        *
    FROM
        (
            SELECT
                accountId,
                endDate AS warranty_expiry_date,
                row_number() OVER (
                    PARTITION BY accountId
                    ORDER BY
                        sync_at DESC
                ) AS rnk
            FROM
                amt.warranty_extensions
        )
    WHERE
        rnk = 1
),
salesforce_cds_data_cte AS (
    SELECT
        DISTINCT lead_record,
        max(DATE(cds1_date)) AS cds1_date,
        max(DATE(cds2_date)) AS cds2_date
    FROM
        salesforce.customer_data_survey
    GROUP BY
        1
),
cds_cte AS (
    SELECT
        DISTINCT leadId,
        max(cds1CompletionDate) AS CDS1_Date__c,
        max(cds2CompletionDate) AS CDS2_Date__c
    FROM
        (
            SELECT
                DISTINCT leadId,
                cds1CompletionDate,
                cds2CompletionDate,
                is_migrated,
                row_number() OVER (
                    PARTITION BY leadId
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                `sales-service`.cds
        )
    WHERE
        rnk = 1
    GROUP BY
        1
),
next_of_kin_details_cte AS (
    SELECT
        *
    FROM
        (
            SELECT
                DISTINCT leadId,
                phoneNumber AS Next_of_Kin_Phone_Number__c,
                firstName AS Next_of_Kin_Name__c,
                row_number() OVER (
                    PARTITION BY leadId
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                `sales-service`.next_of_kin_details
        )
    WHERE
        rnk = 1
),
repossessions_cte AS (
    SELECT
        accountId,
        sum(amount) AS amount
    FROM
        (
            SELECT
                DISTINCT accountId,
                amount,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.repossessions
        )
    WHERE
        rnk = 1
    GROUP BY
        accountId
),
repo_fma_cte AS (
    SELECT
        account_id,
        Repossession_type,
        created_at,
        repossessions_reason
    FROM
        (
            SELECT
                account_id,
                status,
                type AS Repossession_type,
                reason AS repossessions_reason,
                created_at,
                row_number() OVER (
                    PARTITION BY account_id
                    ORDER BY
                        updated_at DESC
                ) AS rnk
            FROM
                fma.repossessions
            WHERE
                status IN ('NEW', 'ASSIGNED', 'ON_HOLD')
        )
    WHERE
        rnk = 1
),
refund AS (
    SELECT
        accountId,
        max(refundDate) AS refund_Date
    FROM
        amt.wallet_installment_payments
    WHERE
        refundDate IS NOT NULL
    GROUP BY
        accountId
),
writeoffs_cte AS (
    SELECT
        accountId,
        sum(amount) AS amount
    FROM
        (
            SELECT
                DISTINCT accountId,
                amount,
                row_number() OVER (
                    PARTITION BY id
                    ORDER BY
                        updatedAt DESC
                ) AS rnk
            FROM
                amt.writeoffs
        )
    WHERE
        rnk = 1
    GROUP BY
        accountId
),
accounts_mv_cte AS (
    SELECT
        DISTINCT a.id AS account_id,
        a.accountRef AS accountRef,
        a.accountType AS accountType,
        a.salesAgents AS salesAgents,
        a.status AS status,
        repo_fma_cte.Repossession_type,
        repo_fma_cte.created_at AS Repossession_created_at,
        repo_fma_cte.repossessions_reason,
        a.accountBalance AS account_balance,
        a.paygContractNumber AS payg_contract_number,
        if(
            toDateTime(rb.salesOrderApprovalDate) IS NULL,
            a.dispatchDate,
            toDateTime(rb.salesOrderApprovalDate)
        ) AS dispatchDate,
        rb.salesOrderNumber AS salesOrderNumber,
        a.fullDepositDate AS fullDepositDate,
        a.jsfDate AS jsfDate,
        nullIf(
            aw.warranty_expiry_date,
            toDateTime('1970-01-01 00:00:00')
        ) AS warranty_expiry_date,
        a.parentAccountId AS parentAccountId,
        a.externalId AS externalId,
        a.createdAt AS createdAt,
        a.updatedAt AS updatedAt,
        a.netSuiteAccountId AS netsuite_account_id,
        ranked_customers.id AS customerId,
        ranked_customers.companyRegion AS companyRegion,
        customer_types_cte.customerType AS customerType,
        ranked_customers.customerTypeId AS customerTypeId,
        ranked_customers.name AS customer_name,
        ranked_customers.phoneNumber AS phoneNumber,
        ranked_customers.identification_number AS identification_number,
        coalesce(kyc_cte.gender, ranked_customers.gender) AS gender,
        coalesce(ranked_customers.nationalIdFrontPic, q.fileName) AS nationalIdFrontPic,
        ranked_customers.documentType AS documentType,
        next_of_kin_details_cte.Next_of_Kin_Name__c AS Next_of_Kin_Name__c,
        next_of_kin_details_cte.Next_of_Kin_Phone_Number__c AS Next_of_Kin_Phone_Number__c,
        a.creditCheck AS creditCheck,
        ranked_customers.creditCheck AS creditCheckCustomer,
        pa.product AS product,
        ap.productQty AS productQty,
        rd.deviceId AS deviceId,
        p.productId AS productId,
        p.id AS payplan_id,
        p.downpayment_amount AS downpayment_amount,
        p.installment_amount AS installment_amount,
        if(
            rsh.installment_date IS NULL,
            a.expectedStartDate,
            rsh.installment_date
        ) AS installment_date,
        p.total_number_payments AS total_number_payments,
        p.name AS payplan_name,
        greatest(
            0,
            dateDiff('month', rsh.installment_date, today()) - if(
                toDayOfMonth(today()) < toDayOfMonth(rsh.installment_date),
                1,
                0
            )
        ) AS expected_payments_number,
        e.id AS employee_id,
        e.name AS employee_name,
        e.createdAt AS employee_createdAt,
        e.status AS employee_status,
        e.gender AS employee_gender,
        rv.name AS relationship_manager,
        e.phoneNumber AS employee_phoneNumber,
        e.identificationNumber AS employee_identificationNumber,
        rs.id AS supervisor_id,
        rs.name AS supervisor_name,
        rm.name AS RSM,
        d.name AS Department_name,
        ranked_sales.sale_date AS Full_Deposit_Date,
        multiIf(
            (ranked_customers.companyRegion = 'uganda')
            AND (ranked_sales.sale_date IS NOT NULL)
            AND (ranked_sales.sale_date < toDate('2026-03-01'))
            AND (a.accountType = 'PAYG'),
            toTimeZone(cds_cte.CDS2_Date__c, 'UTC'),
            (ranked_customers.companyRegion = 'uganda')
            AND (ranked_sales.sale_date >= toDate('2024-10-01'))
            AND (ranked_sales.sale_date <= toDate('2026-03-01'))
            AND (a.accountType = 'CASH'),
            toTimeZone(cds_cte.CDS1_Date__c, 'UTC'),
            (a.accountType = 'ADDON')
            AND (a.status != 'No Deposit'),
            a.createdAt,
            ranked_sales.sale_date
        ) AS sale_date,
        aggregated_payments.PaidAmount AS PaidAmount,
        aggregated_payments.first_payment_date AS first_payment_date,
        aggregated_payments.last_payment_date AS last_payment_date,
        aggregated_discounts.discountAmount AS discountAmount,
        r.refundAmount AS refundAmount,
        coalesce(aggregated_payments.PaidAmount, 0) AS Total_Paid,
        coalesce(r.RefundDate, refund.refund_Date) AS RefundDate,
        ranked_leads.leadId AS leadId,
        ranked_leads.LeadSource AS LeadSource,
        ranked_leads.referralType AS referralType,
        ranked_leads.Referral_ID__c AS Referral_ID__c,
        ranked_leads.Referral_Name__c AS Referral_Name__c,
        ranked_leads.Referral_Phone_Number__c AS Referral_Phone_Number__c,
        greatest(
            coalesce(a.updatedAt, toDateTime('1970-01-01 00:00:00')),
            coalesce(
                ranked_sales.sale_date,
                toDateTime('1970-01-01 00:00:00')
            ),
            toDateTime('1970-01-01 00:00:00')
        ),
        coalesce(
            aggregated_payments.last_payment_date,
            toDateTime('1970-01-01 00:00:00')
        ) AS last_modified,
        repossessions_cte.amount AS repossession_amount,
        writeoffs_cte.amount AS writeoffs_amount,
        ranked_customers.isMigrated AS customerWalletIsMigrated,
        coalesce(
            DATE(cds_cte.CDS1_Date__c),
            DATE(salesforce_cds_data_cte.cds1_date)
        ) AS cds1_date,
        coalesce(
            DATE(cds_cte.CDS2_Date__c),
            DATE(salesforce_cds_data_cte.cds2_date)
        ) AS cds2_date
    FROM
        ranked_accounts AS a
        LEFT JOIN ranked_customers ON a.customerId = ranked_customers.id
        LEFT JOIN customer_types_cte ON customer_types_cte.id = ranked_customers.customerTypeId
        LEFT JOIN ranked_leads ON ranked_customers.identification_number = ranked_leads.ID_Number__c
        LEFT JOIN cds_cte ON cds_cte.leadId = ranked_leads.leadId
        LEFT JOIN salesforce_cds_data_cte ON salesforce_cds_data_cte.lead_record = cds_cte.leadId
        LEFT JOIN next_of_kin_details_cte ON next_of_kin_details_cte.leadId = ranked_leads.leadId
        LEFT JOIN ranked_s3 AS q ON ranked_leads.leadId = q.accountId
        LEFT JOIN ranked_sales ON a.id = ranked_sales.account_id
        LEFT JOIN ranked_employees AS e ON a.salesAgents = e.id
        LEFT JOIN ranked_departments AS d ON d.id = e.departmentId
        LEFT JOIN ranked_account_device AS rd ON a.id = rd.accountId
        LEFT JOIN aggregated_refunds AS r ON a.id = r.accountId
        LEFT JOIN ranked_account_payplans AS ap ON a.id = ap.accountId
        LEFT JOIN ranked_payplans AS p ON p.id = ap.payplanId
        LEFT JOIN ranked_products AS pa ON p.productId = pa.id
        LEFT JOIN kyc_cte ON kyc_cte.idNumber = ranked_customers.identification_number
        LEFT JOIN ranked_supervisor AS rs ON e.supervisorId = rs.id
        LEFT JOIN ranked_rm AS rm ON rs.supervisorId = rm.id
        LEFT JOIN aggregated_discounts ON a.id = aggregated_discounts.accountId
        LEFT JOIN aggregated_payments ON a.id = aggregated_payments.accountId
        LEFT JOIN ranked_view AS rv ON ranked_customers.id = rv.customerId
        LEFT JOIN ranked_schedules AS rsh ON a.id = rsh.account_id
        LEFT JOIN aggregated_warranty AS aw ON a.id = aw.accountId
        LEFT JOIN ranked_boq AS rb ON a.id = rb.accountId
        LEFT JOIN repossessions_cte ON repossessions_cte.accountId = a.id
        LEFT JOIN writeoffs_cte ON writeoffs_cte.accountId = a.id
        LEFT JOIN repo_fma_cte ON repo_fma_cte.account_id = a.id
        LEFT JOIN refund ON refund.accountId = a.id
)
SELECT
    *
FROM
    accounts_mv_cte
WHERE
    product NOT IN (
        'Transport / Shipping',
        'TSR',
        'TSR Uganda',
        'Training',
        'UG Extra Items',
        'Extra Items',
        'Installation',
        'furrow',
        'AfterSale',
        'Agronomy',
        'Samsung Galaxy A11'
    )