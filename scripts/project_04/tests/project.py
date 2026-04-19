"""
Oracle to Salesforce Account Synchronization Lambda Function

This Lambda function synchronizes account data from Oracle to Salesforce by:
1. Extracting unprocessed records from Oracle
2. Deduplicating records
3. Enriching with Salesforce reference data
4. Upserting/updating records in Salesforce
5. Logging results back to Oracle
"""

import json
from typing import Any, Tuple

import pandas as pd
from aws import get_secret
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from oracle import load_data_frame_from_oracle, update_data_frame_to_oracle
from salesforce import (
    load_data_frame_from_salesforce,
    merge_salesforce_job_results,
    salesforce_bulk_update,
    salesforce_bulk_upsert,
)

logger = Logger()


# ============================================================================
# SQL QUERIES
# ============================================================================

SQL_QUERY = """
    SELECT  
        OUT_ACCOUNT_ID,
        OUT_ACCOUNT_ACTION,
        ID,
        NAME,
        RECORDTYPE,
        "TYPE",
        PARENTID,
        BILLINGSTREET,
        BILLINGCITY,
        BILLINGSTATE,
        BILLINGPOSTALCODE,
        BILLINGCOUNTRY,
        SHIPPINGSTREET,
        SHIPPINGCITY,
        SHIPPINGSTATE,
        SHIPPINGPOSTALCODE,
        SHIPPINGCOUNTRY,
        PHONE,
        FAX,
        ACCOUNTNUMBER,
        WEBSITE,
        COALESCE(ANNUALREVENUE, '0') AS ANNUALREVENUE,
        COALESCE(NUMBEROFEMPLOYEES, 0) AS NUMBEROFEMPLOYEES,
        OWNERSHIP,
        TICKERSYMBOL,
        DESCRIPTION,
        RATING,
        OWNERID,
        TRACECODE,
        CTACCTNUMBER,
        CTACCOUNTTYPE,
        TRIM(LASTBWUSAGE) AS LASTBWUSAGE,
        MEMBERTYPE,
        MEMBERDATE,
        CREDITSTATUS,
        CURRENCY,
        LANDEDSALESREPID,
        MAINTENANCESALESREPID,
        CTSTATUS,
        EBS_PARTY_ID,
        EBS_CUST_ACCOUNT_ID,
        EBS_AGENCY_ID,
        EBS_MASTER_ORG_PARTY_ID,
        CREATION_TIMESTAMP,
        UPDATE_TIMESTAMP,
        PROCESSED,
        SFDC_ERROR,
        LASTBWUSAGEDATE,
        TRIM(FIRST_INVOICE_DATE) AS FIRST_INVOICE_DATE,
        TRIM(FIRST_PAYMENT_DATE) AS FIRST_PAYMENT_DATE,
        TRIM(ACCOUNT_MODE) AS ACCOUNT_MODE,
        TRIM(NBD_BASE_FLAG) AS NBD_BASE_FLAG,
        PARTNERSHIP,
        BUSINESS_UNIT,
        ORG_ID
    FROM SFORCE.OUT_ACCOUNT
    WHERE PROCESSED = '0'
"""


# ============================================================================
# SALESFORCE QUERY CONFIGURATION
# ============================================================================

SOQL_QUERY_MAP = {
    "record_type": {
        "query_template": "SELECT Id, Name FROM RecordType WHERE Name IN ({})",
        "column_name": "RECORDTYPE",
        "join_on": ("RECORDTYPE", "RecordTypeName"),
        "rename_map": {
            "Id": "RecordTypeId",
            "Name": "RecordTypeName"
        },
        "expected_columns": ["RecordTypeId", "RecordTypeName"]
    },
    "account_agency": {
        "query_template": "SELECT Id, EBS_Cust_Account_ID__c FROM Account WHERE EBS_Cust_Account_ID__c in ({})",
        "column_name": "EBS_AGENCY_ID",
        "join_on": ("EBS_AGENCY_ID", "EBS_CUST_ACCOUNT_ID_C"),
        "rename_map": {
            "Id": "Agency_Account__c",
            "EBS_Cust_Account_ID__c": "EBS_CUST_ACCOUNT_ID_C"
        },
        "expected_columns": ["Agency_Account__c", "EBS_CUST_ACCOUNT_ID_C"]
    },
    "account_name": {
        "query_template": "SELECT Name, AccountNumber FROM Account WHERE AccountNumber in ({})",
        "column_name": "ACCOUNTNUMBER",
        "join_on": ("ACCOUNTNUMBER", "ACCOUNT_NUMBER"),
        "rename_map": {
            "Name": "AccountName",
            "AccountNumber": "ACCOUNT_NUMBER"
        },
        "expected_columns": ["AccountName", "ACCOUNT_NUMBER"],
    },
    "user_owner": {
        "query_template": "SELECT Id, Salesrep_ID__c, Contact_ID__c FROM User WHERE Salesrep_ID__c in ({})",
        "column_name": "OWNERID",
        "join_on": ("OWNERID", "Salesrep_ID__c"),
        "rename_map": {"Id": "OwnerId"},
        "expected_columns": ["OwnerId", "Salesrep_ID__c", "Contact_ID__c"],
    },
    "user_landed": {
        "query_template": "SELECT Salesrep_ID__c, Contact_ID__c FROM User WHERE Salesrep_ID__c in ({})",
        "column_name": "LANDEDSALESREPID",
        "join_on": ("LANDEDSALESREPID", "Salesrep_ID__c"),
        "rename_map": {"Contact_ID__c": "Current_Landed_By__c"},
        "expected_columns": ["Salesrep_ID__c", "Current_Landed_By__c"]
    },
    "user_maint_by": {
        "query_template": "SELECT Salesrep_ID__c, Contact_ID__c FROM User WHERE Salesrep_ID__c in ({})",
        "column_name": "MAINTENANCESALESREPID",
        "join_on": ("MAINTENANCESALESREPID", "Salesrep_ID__c"),
        "rename_map": {"Contact_ID__c": "Current_Maintained_By__c"},
        "expected_columns": ["Salesrep_ID__c", "Current_Maintained_By__c"]
    },
}


# ============================================================================
# COLUMN MAPPINGS
# ============================================================================

COLUMN_MAPPING = {
    "OUT_ACCOUNT_ACTION": "OUT_ACCOUNT_ACTION",
    "ID": "Id",
    "NAME": "ClientTrak_Account_Name__c",
    "TYPE": "Type",
    "PARENTID": "ParentId",
    "BILLINGSTREET": "BillingStreet",
    "BILLINGCITY": "BillingCity",
    "BILLINGSTATE": "BillingState",
    "BILLINGPOSTALCODE": "BillingPostalCode",
    "BILLINGCOUNTRY": "BillingCountry",
    "SHIPPINGSTREET": "ShippingStreet",
    "SHIPPINGCITY": "ShippingCity",
    "SHIPPINGSTATE": "ShippingState",
    "SHIPPINGPOSTALCODE": "ShippingPostalCode",
    "SHIPPINGCOUNTRY": "ShippingCountry",
    "PHONE": "Phone",
    "FAX": "Fax",
    "ACCOUNTNUMBER": "AccountNumber",
    "WEBSITE": "Website",
    "ANNUALREVENUE": "AnnualRevenue",
    "NUMBEROFEMPLOYEES": "NumberOfEmployees",
    "OWNERSHIP": "Ownership",
    "TICKERSYMBOL": "TickerSymbol",
    "DESCRIPTION": "Description",
    "RATING": "Rating",
    "TRACECODE": "Trace_Code__c",
    "CTACCTNUMBER": "CT_Account_Number__c",
    "CTACCOUNTTYPE": "CT_Account_Type__c",
    "LASTBWUSAGE": "Last_BW_Usage__c",
    "MEMBERTYPE": "Member_Type__c",
    "MEMBERDATE": "Member_Date__c",
    "CREDITSTATUS": "Credit_Status__c",
    "CURRENCY": "Currency__c",
    "LANDEDSALESREPID": "DUNS_Number__c",
    "CTSTATUS": "Status__c",
    "EBS_PARTY_ID": "EBS_Party_ID__c",
    "EBS_CUST_ACCOUNT_ID": "EBS_Cust_Account_ID__c",
    "EBS_AGENCY_ID": "EBS_AGENCY_ID__c",
    "EBS_MASTER_ORG_PARTY_ID": "EBS_Master_Org_Party_ID__c",
    "LASTBWUSAGEDATE": "Last_BW_Usage_Date__c",
    "FIRST_INVOICE_DATE": "First_Invoice_Date__c",
    "FIRST_PAYMENT_DATE": "First_Payment_Date__c",
    "ACCOUNT_MODE": "Account_Mode__c",
    "NBD_BASE_FLAG": "NBD_Base_Flag__c",
    "BUSINESS_UNIT": "Business_Unit__c",
    "ORG_ID": "Org_ID__c",
    "FIRST_INVOICE_DATE_FORMATTED": "First_Invoice_Date_Format__c",
    "FIRST_PAYMENT_DATE_FORMATTED": "First_Payment_Date_Format__c",
    "LASTBWUSAGEDATE_FORMATTED": "Last_BW_Usage_Date_Format__c",
    "MEMBERDATE_FORMATTED": "Member_Date_Format__c"
}

SALESFORCE_COLUMNS = [
    "Id",
    "Name",
    "ClientTrak_Account_Name__c",
    "Type",
    "ParentId",
    "BillingStreet",
    "BillingCity",
    "BillingState",
    "BillingPostalCode",
    "BillingCountry",
    "ShippingStreet",
    "ShippingCity",
    "ShippingState",
    "ShippingPostalCode",
    "ShippingCountry",
    "Phone",
    "Fax",
    "AccountNumber",
    "Website",
    "AnnualRevenue",
    "NumberOfEmployees",
    "Ownership",
    "TickerSymbol",
    "Description",
    "Rating",
    "Trace_Code__c",
    "CT_Account_Number__c",
    "CT_Account_Type__c",
    "Last_BW_Usage__c",
    "Member_Type__c",
    "Member_Date__c",
    "Credit_Status__c",
    "Currency__c",
    "Status__c",
    "EBS_Party_ID__c",
    "EBS_Cust_Account_ID__c",
    "EBS_AGENCY_ID__c",
    "Last_BW_Usage_Date__c",
    "First_Invoice_Date__c",
    "First_Payment_Date__c",
    "Account_Mode__c",
    "NBD_Base_Flag__c",
    "Business_Unit__c",
    "Org_ID__c",
    "First_Invoice_Date_Format__c",
    "First_Payment_Date_Format__c",
    "Last_BW_Usage_Date_Format__c",
    "Member_Date_Format__c",
    "OwnerId",
    "Current_Landed_By__c",
    "Current_Maintained_By__c",
    "Agency_Account__c"
]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def deduplicate_dataframe(
    df: pd.DataFrame,
    dedup_columns: list[str],
    sort_by: list[str] = None,
    ascending: bool | list[bool] = True
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Splits the DataFrame into original (unique) and duplicate records based on specified columns,
    optionally sorting before deduplication.

    Args:
        df: The input DataFrame
        dedup_columns: Columns to use for identifying duplicates
        sort_by: Columns to sort by before deduplication (optional)
        ascending: Sort order (default: True)

    Returns:
        Tuple containing:
            - original_df: First occurrence of each unique record
            - duplicate_df: All subsequent duplicate records
    """
    # Sort if sort_by is specified
    if sort_by:
        df = df.sort_values(by=sort_by, ascending=ascending).reset_index(drop=True)

    # Identify duplicates
    is_duplicate = df.duplicated(subset=dedup_columns, keep='first')
    is_first = ~is_duplicate

    # Split DataFrame
    original_df = df[is_first].reset_index(drop=True)
    duplicate_df = df[is_duplicate].reset_index(drop=True)

    return original_df, duplicate_df


def prepare_data_frame(
    df: pd.DataFrame,
    column_mapping: dict,
    result_columns: list
) -> pd.DataFrame:
    """
    Prepare DataFrame before uploading to Salesforce by:
    - Setting Name field based on action type
    - Cleaning empty string values
    - Formatting date fields
    - Converting numeric fields
    - Renaming columns

    Args:
        df: DataFrame to prepare
        column_mapping: Dictionary mapping Oracle to Salesforce column names
        result_columns: List of final columns to include

    Returns:
        Transformed DataFrame ready for Salesforce upload
    """
    result_df = df.copy()

    # Set Name field - use NAME for INSERT or if no existing AccountName
    result_df["Name"] = result_df.apply(
        lambda row: row["NAME"] 
        if row["OUT_ACCOUNT_ACTION"] == "INSERT" or pd.isna(row["AccountName"]) 
        else None,
        axis=1
    )

    # Replace empty strings with None for specific fields
    result_df["ACCOUNT_MODE"] = result_df["ACCOUNT_MODE"].replace("", None)
    result_df["LASTBWUSAGE"] = result_df["LASTBWUSAGE"].replace("", None)
    result_df["NBD_BASE_FLAG"] = result_df["NBD_BASE_FLAG"].replace("", None)

    # Format date fields from Oracle format (DD-MON-YYYY) to Salesforce format (YYYY-MM-DD)
    date_fields = [
        ("FIRST_INVOICE_DATE", "FIRST_INVOICE_DATE_FORMATTED"),
        ("FIRST_PAYMENT_DATE", "FIRST_PAYMENT_DATE_FORMATTED"),
        ("LASTBWUSAGEDATE", "LASTBWUSAGEDATE_FORMATTED"),
        ("MEMBERDATE", "MEMBERDATE_FORMATTED")
    ]

    for source_field, target_field in date_fields:
        result_df[target_field] = (
            pd.to_datetime(
                df[source_field],
                format="%d-%b-%Y",
                errors="coerce"
            )
            .dt.strftime("%Y-%m-%d")
            .fillna('')
            .astype(str)
        )

    # Convert EBS fields to Int64 (handles nulls properly)
    result_df["EBS_PARTY_ID"] = (
        pd.to_numeric(result_df["EBS_PARTY_ID"], errors="coerce")
        .where(lambda s: s.ne(0), pd.NA)
        .astype("Int64")
    )

    result_df["EBS_CUST_ACCOUNT_ID"] = (
        pd.to_numeric(result_df["EBS_CUST_ACCOUNT_ID"], errors="coerce")
        .where(lambda s: s.ne(0), pd.NA)
        .astype("Int64")
    )

    # Rename columns to Salesforce field names
    result_df.rename(columns=column_mapping, inplace=True, errors="raise")
    
    # Select only required columns
    result_df = result_df[result_columns]

    return result_df


def get_account_data_from_salesforce(
    salesforce_config: dict,
    source_df: pd.DataFrame,
    query_map: dict,
    column_mapping: dict,
    result_columns: list
) -> pd.DataFrame:
    """
    Fetch all required data from Salesforce and merge with source data.
    
    This function:
    1. Queries Salesforce for reference data (RecordTypes, Users, Accounts)
    2. Merges the data with source DataFrame
    3. Transforms and formats the data for upload

    Args:
        salesforce_config: Salesforce connection details
        source_df: DataFrame with source data from Oracle
        query_map: Dictionary with Salesforce query configurations
        column_mapping: Dictionary mapping Oracle to Salesforce column names
        result_columns: List of final columns to include

    Returns:
        Transformed DataFrame with all Salesforce reference data merged
    """
    result_df = source_df.copy()

    # Process each query configuration
    for config in query_map.values():
        # Get unique values from source data for this query
        if config["column_name"] == "EBS_AGENCY_ID":
            values = result_df[config["column_name"]].dropna().astype("Int64").unique()
        else:
            values = result_df[config["column_name"]].dropna().unique()

        # Only query if there are values to search for
        if values.any():
            # Query Salesforce in batches to respect governor limits
            batch_size = 200
            dfs = []
            
            for i in range(0, len(values), batch_size):
                batch = values[i : i + batch_size]
                in_clause = ", ".join(f"'{v}'" for v in batch)
                soql = config["query_template"].format(in_clause)
                sf_df = load_data_frame_from_salesforce(salesforce_config, soql)
                dfs.append(sf_df)

            sf_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        else:
            sf_df = pd.DataFrame()

        # Merge Salesforce data if found
        if not sf_df.empty:
            # Rename columns if needed
            if "rename_map" in config:
                sf_df.rename(columns=config["rename_map"], inplace=True)
                left_on = config["join_on"][0]
                right_on = config["rename_map"].get(
                    config["join_on"][1], 
                    config["join_on"][1]
                )
            else:
                left_on, right_on = config["join_on"]

            # Ensure matching data types for join
            if result_df[left_on].dtype != sf_df[right_on].dtype:
                result_df[left_on] = result_df[left_on].astype(str)
                sf_df[right_on] = sf_df[right_on].astype(str)

            # Special handling for EBS_AGENCY_ID (numeric field)
            if config["column_name"] == "EBS_AGENCY_ID":
                result_df["EBS_AGENCY_ID"] = (
                    pd.to_numeric(result_df["EBS_AGENCY_ID"], errors="coerce")
                    .where(lambda s: s.ne(0), pd.NA)
                    .astype("Int64")
                )
                
                sf_df["EBS_CUST_ACCOUNT_ID_C"] = (
                    pd.to_numeric(sf_df["EBS_CUST_ACCOUNT_ID_C"], errors="coerce")
                    .where(lambda s: s.ne(0), pd.NA)
                    .astype("Int64")
                )

                # Convert join keys to Int64
                for df_, key in ((result_df, left_on), (sf_df, right_on)):
                    if key in df_.columns:
                        df_[key] = pd.to_numeric(df_[key], errors="coerce").astype("Int64")

                # Merge data
                result_df = result_df.merge(
                    sf_df, 
                    how="left", 
                    left_on=left_on, 
                    right_on=right_on
                )

                # Handle null values in EBS_AGENCY_ID
                _m = result_df["EBS_AGENCY_ID"].isna()
                result_df["EBS_AGENCY_ID"] = result_df["EBS_AGENCY_ID"].astype(object)
                result_df.loc[_m, "EBS_AGENCY_ID"] = None
            else:
                # Standard merge for other fields
                result_df = result_df.merge(
                    sf_df, 
                    how="left", 
                    left_on=left_on, 
                    right_on=right_on
                )
        else:
            # Add expected columns with None values if no data found
            for col in config["expected_columns"]:
                if col not in result_df.columns:
                    result_df[col] = None

    # Final transformation
    result_df = prepare_data_frame(result_df, column_mapping, result_columns)

    return result_df


def upload_account_data(
    salesforce_config: dict,
    oracle_config: dict,
    df: pd.DataFrame,
    action: str,
    external_id_field: str = "Id"
) -> None:
    """
    Upload account data to Salesforce and log results back to Oracle.
    
    This function:
    1. Performs bulk upsert or update in Salesforce
    2. Retrieves job results (success and failures)
    3. Updates Oracle with processing status and errors

    Args:
        salesforce_config: Salesforce connection details
        oracle_config: Oracle connection details
        df: DataFrame with data to upload
        action: "update" or "upsert"
        external_id_field: Field to use for matching records (default: "Id")

    Raises:
        ValueError: If action is not "update" or "upsert"
    """
    # Validate action
    if action not in ["upsert", "update"]:
        raise ValueError(
            f"Invalid action: {action}. Action must be either 'update' or 'upsert'."
        )

    # Execute Salesforce bulk operation
    if action == "upsert":
        job_id = salesforce_bulk_upsert(
            salesforce_config, 
            "Account", 
            df, 
            external_id_field
        )
    else:
        job_id = salesforce_bulk_update(
            salesforce_config, 
            "Account", 
            df, 
            external_id_field
        )

    # Get job results
    df_success, df_failed = merge_salesforce_job_results(job_id, salesforce_config)

    # Update Oracle with results if there are any
    if not (df_success.empty and df_failed.empty):
        # Prepare success records
        df_success = df_success[["SFDC_ERROR", "PROCESSED", "EBS_Party_ID__c"]].rename(
            columns={"EBS_Party_ID__c": "EBS_PARTY_ID"}
        )
        
        # Prepare failed records
        df_failed = df_failed[["SFDC_ERROR", "PROCESSED", "EBS_Party_ID__c"]].rename(
            columns={"EBS_Party_ID__c": "EBS_PARTY_ID"}
        )

        # Combine results
        merged_df = pd.concat([df_success, df_failed], ignore_index=True)

        # Update Oracle table with processing status
        update_data_frame_to_oracle(
            df=merged_df,
            oracle_config=oracle_config,
            join_conditions={"EBS_PARTY_ID": "="},
            additional_conditions=["PROCESSED = '0'"],
            update_columns=["SFDC_ERROR", "PROCESSED"],
            schema_name="SFORCE",
            table_name="OUT_ACCOUNT",
        )


# ============================================================================
# MAIN LAMBDA HANDLER
# ============================================================================

@logger.inject_lambda_context
def lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:
    """
    AWS Lambda handler for Oracle to Salesforce account synchronization.
    
    Process Flow:
    1. Load configuration from AWS Secrets Manager
    2. Extract unprocessed records from Oracle
    3. Deduplicate records by EBS_PARTY_ID
    4. Enrich with Salesforce reference data
    5. Split records by whether they have a Salesforce Id
    6. Upsert records without Id (using EBS_Party_ID__c)
    7. Update records with Id
    8. Log all results back to Oracle

    Args:
        event: Lambda event containing:
            - salesforce_secret_name: Name of Salesforce credentials secret
            - oracle_secret_name: Name of Oracle credentials secret
        context: Lambda context object

    Returns:
        Response with status code and message
    """
    # ========================================================================
    # STEP 1: Load Configuration
    # ========================================================================
    logger.info("Loading configuration from AWS Secrets Manager")
    salesforce_config = get_secret(event["salesforce_secret_name"])
    oracle_config = get_secret(event["oracle_secret_name"])

    # ========================================================================
    # STEP 2: Extract Data from Oracle
    # ========================================================================
    logger.info("Extracting unprocessed records from Oracle")
    source_df = load_data_frame_from_oracle(oracle_config, SQL_QUERY)
    
    if source_df.empty:
        logger.info("No unprocessed records found in Oracle")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No rows returned by oracle query'})
        }
    
    logger.info(f"Loaded {len(source_df)} records from Oracle")

    # ========================================================================
    # STEP 3: Deduplicate Records
    # ========================================================================
    logger.info("Deduplicating records by EBS_PARTY_ID")
    df, duplicates_df = deduplicate_dataframe(
        source_df,
        dedup_columns=["EBS_PARTY_ID"],
        sort_by=["UPDATE_TIMESTAMP", "CREATION_TIMESTAMP"],
        ascending=[False, False]  # Most recent first
    )
    
    logger.info(f"Found {len(duplicates_df)} duplicate records")

    # Mark duplicate records in Oracle
    if not duplicates_df.empty:
        logger.info("Marking duplicate records in Oracle")
        update_data_frame_to_oracle(
            df=duplicates_df,
            oracle_config=oracle_config,
            join_conditions={"OUT_ACCOUNT_ID": "="},
            static_updates={
                "PROCESSED": "D",
                "SFDC_ERROR": (
                    "Duplicate Account ID in batch. This row is being skipped. "
                    "Batch jobs can only have one account update per execution."
                )
            },
            schema_name="SFORCE",
            table_name="OUT_ACCOUNT"
        )

    # ========================================================================
    # STEP 4: Enrich with Salesforce Data
    # ========================================================================
    logger.info("Enriching data with Salesforce reference data")
    processed_df = get_account_data_from_salesforce(
        salesforce_config,
        df,
        SOQL_QUERY_MAP,
        COLUMN_MAPPING,
        SALESFORCE_COLUMNS
    )

    # ========================================================================
    # STEP 5: Split Records by Id Presence
    # ========================================================================
    logger.info("Splitting records based on Salesforce Id presence")
    
    # Records without Salesforce Id - need to upsert by EBS_Party_ID__c
    df_null_id = processed_df[processed_df["Id"].isnull()]
    df_null_id = df_null_id[df_null_id["EBS_Party_ID__c"].notnull()].fillna('')
    
    # Records with Salesforce Id - can update directly
    df_not_null_id = processed_df[processed_df["Id"].notnull()].fillna('')

    logger.info(f"Records to upsert (no Id): {len(df_null_id)}")
    logger.info(f"Records to update (has Id): {len(df_not_null_id)}")

    # ========================================================================
    # STEP 6: Upload to Salesforce
    # ========================================================================
    
    # Upsert records without Salesforce Id
    if not df_null_id.empty:
        logger.info("Upserting records by EBS_Party_ID__c")
        upload_account_data(
            salesforce_config,
            oracle_config,
            df_null_id,
            action="upsert",
            external_id_field="EBS_Party_ID__c"
        )
    
    # Update records with Salesforce Id
    if not df_not_null_id.empty:
        logger.info("Updating records by Id")
        upload_account_data(
            salesforce_config,
            oracle_config,
            df_not_null_id,
            action="update",
            external_id_field="Id"
        )

    # ========================================================================
    # STEP 7: Return Success
    # ========================================================================
    logger.info("Synchronization completed successfully")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': (
                'Data was successfully moved from Oracle to Salesforce. '
                'Changes were logged in Oracle tables.'
            )
        })
    }