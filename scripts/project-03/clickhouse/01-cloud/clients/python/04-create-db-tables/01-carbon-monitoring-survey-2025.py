import os
from dotenv import load_dotenv
import clickhouse_connect

# Load environment variables from .env file
#load_dotenv()
load_dotenv(override=True)  # Force reload environment variables


# Retrieve credentials
clickhouse_cloud_host = os.getenv("clickhouse_cloud_host")
clickhouse_cloud_user = os.getenv("clickhouse_cloud_user")
clickhouse_cloud_user_password = os.getenv("clickhouse_cloud_user_password")
clickhouse_db = os.getenv("clickhouse_sunculture_db")  # Specify your target database

# Debug: Print environment variables
#print(f"Host: {clickhouse_cloud_host}")
#print(f"User: {clickhouse_cloud_user}")
#print(f"Password: {clickhouse_cloud_user_password}")

def connect_to_clickhouse_cloud():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_cloud_host,
            user=clickhouse_cloud_user, 
            password=clickhouse_cloud_user_password,
            database=clickhouse_db,  # Specify the database
            secure=True # Ensures SSL connection
        )
        return client
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None
    
def create_db_table(client, table_name):
    """
    Creates a ClickHouse table if it doesn't exist.
    """
    create_db_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        _id String,
        formhub_uuid String,
        today Date,
        deviceid String,
        date Date,
        name_of_surveyor String,
        farmer_consented_to_the_interview String,
        customer_id String,
        customer_name String,
        customer_sunculture_region String,
        customer_county String,
        customer_s_address_village String,
        age_of_customer Int16,
        gender_of_customer String,
        respondent_name String,
        respondent_s_telephone_number String,
        alternative_phone_number String,
        model_of_sunculture_pump_being_used String,
        get_imei String,
        device_id_imei String,
        pump_start_run_time String,
        sunculture_pump_date_and_year_of_purchase Date,
        satus_of_sunculture_solar_pump String,
        previous_own_pump String,
        pumptype String,
        vintage_interviewed String,
        monthly_spending_suncuturepump Float64,
        yearly_yield_baselinepump_001 Float64,
        time_spent_field_sunculturepum Float64,
        no_of_women_involve_sing_sunculture_pump_001 Float64,
        daysdryseason Float64,
        hoursdryseason Float64,
        daysrainyseason Float64,
        hoursrainyseason Float64,
        harvestsold_sunculture Float64,
        did_you_use_sprinkle_the_pumptype_pump String,
        are_you_using_sprink_the_sunculture_pump String,
        additional_comments_for_the_survey String,
        issues_questions_c_low_up_by_sunculture String,
        iot_firmware_protocal String,
        iot_device_variant String,
        iot_firmware_version Float64,
        iot_upload_interval_in_minutes Float64,
        iot_days_since_last_report Float64,
        iot_fetch_pump_utilization_data String,
        iot_pump_run_end_time String,
        record_your_current_location String,
        __version__ String,
        meta_instanceid String,
        _xform_id_string String,
        _uuid String,
        _status String,
        _geolocation String,
        _submission_time DateTime,
        _submitted_by String
    ) ENGINE = MergeTree()
    order by _uuid
    """
    try:
        client.command(create_db_table_query)
        print(f"Table '{table_name}' created successfully!")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

# Main execution
if __name__ == "__main__":
    # Establish connection
    client = connect_to_clickhouse_cloud()
    
    if client:
        # Perform additional operations here
        print("✅ Connected to ClickHouse Cloud!")
        table_name = "carbon_3rd_monitoring_survey_2025"
        
        # Ensure the database exists before creating the table
        client.command(f"CREATE DATABASE IF NOT EXISTS {clickhouse_db}")
        print(f"✅ Database '{clickhouse_db}' is ready!")
        
        # Create table 
        create_db_table(client, table_name)  
    else:
        print("❌ Failed to connect to ClickHouse Cloud.")