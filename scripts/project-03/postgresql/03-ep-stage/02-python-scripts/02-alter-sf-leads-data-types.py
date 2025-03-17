import os
import psycopg2
from psycopg2 import sql


# Database connection parameters
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

# Table and column details
table_name = "sf_leads"

# Dictionary containing column names and their new data types
columns_to_update = {
    "id" : "VARCHAR(255)",
    "masterrecordid": "VARCHAR(255)",
    "phone": "VARCHAR(255)",
    "mobilephone": "VARCHAR(255)",
    "ownerid" : "VARCHAR(255)",
    "convertedaccountid" : "VARCHAR(255)",
    "convertedcontactid" : "VARCHAR(255)",
    "convertedopportunityid" : "VARCHAR(255)",
    "createdbyid" : "VARCHAR(255)",
    "lastmodifiedbyid" : "VARCHAR(255)",
    "purchase_date__c" : "VARCHAR(255)",
    "product__c" : "VARCHAR(255)",
    "agent__c" : "VARCHAR(255)",
    "last_updated_by__c" : "VARCHAR(255)",
    "referral_phone_number__c" : "VARCHAR(255)",
    "through_partner_lead__c" : "VARCHAR(255)",
    "through_partner_customer__c" : "VARCHAR(255)",
    "sadm_account__c" : "VARCHAR(255)",
    "sadm_cds_id__c" : "VARCHAR(255)",
    "sadm_customer__c" : "VARCHAR(255)"
}

# Connect to the database
try:
    connection = psycopg2.connect(**ep_stage_db_params)
    cursor = connection.cursor()

    # Construct and execute ALTER TABLE commands for each column
    for column_name, new_data_type in columns_to_update.items():
        alter_query = sql.SQL("""
            ALTER TABLE {table}
            ALTER COLUMN {column} TYPE {new_type};
        """).format(
            table=sql.Identifier(table_name),
            column=sql.Identifier(column_name),
            new_type=sql.SQL(new_data_type)
        )

        cursor.execute(alter_query)
        print(f"Column '{column_name}' updated to {new_data_type}.")

    # Commit the changes
    connection.commit()
    print("All column updates committed successfully.")

except Exception as e:
    print(f"Error updating schema: {e}")

finally:
    if connection:
        cursor.close()
        connection.close()