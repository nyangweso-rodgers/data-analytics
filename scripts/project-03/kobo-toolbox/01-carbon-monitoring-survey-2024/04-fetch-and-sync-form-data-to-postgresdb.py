import os
from dotenv import load_dotenv
import requests
import psycopg2

# Load environment variables
load_dotenv()

# KoboToolbox API details
KOBO_API_URL = os.getenv("Carbon_2nd_Monitoring_Survey_2024")
KOBO_API_TOKEN = os.getenv("KOBO_API_TOKEN")
HEADERS = {
    "Authorization": f"Token {KOBO_API_TOKEN}",
    "Accept": "application/json"
}

# Database connection parameters
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

# Specify the fields to include in the extracted data
INCLUDED_FIELDS = {
    "date",
    "Name_of_surveyor",
    "Farmer_consented_to_the_interview",
    "Customer_ID",
    "Customer_Name",
    "Customer_SunCulture_Region",
    "Customer_County",
    "Customer_s_address_Village",
    "Age_of_customer",
    "Gender_of_customer",
    "Respondent_Name",
    "Respondent_s_telephone_number",
    "Alternative_Phone_Number",
    "Model_of_SunCulture_pump_being_used",
    "SunCulture_Pump_date_and_year_of_purchase",
    "Satus_of_SunCulture_Solar_pump",
    "Did_you_previously_own_a_diese",
    "How_much_money_is_sp_mp_monthly_average",
    "Total_yearly_yield_b_sing_Sunculture_pump",
    "Total_yearly_yield_n_sing_SunCulture_pump",
    "Time_spent_on_field_sing_Sunculture_pump",
    "Time_spent_on_field_sing_Sunculture_pump_001",
    "No_of_women_involve_sing_SunCulture_pump",
    "No_of_women_involve_sing_SunCulture_pump_001",
    "DaysDrySeason",
    "How_many_hours_per_day_do_you_",
    "DaysRainySeason",
    "How_many_hours_per_day_do_you__001",
    "How_much_harvest_was_e_of_Sunculture_pump",
    "How_much_harvest_is_sing_SunCulture_pump",
    "Did_you_use_sprinkle_the_pumptype_pump",
    #"Take_photo_of_the_SunCulture_pump",
    ##"Take_photo_of_farmer_e_farmer_for_consent",
    "Additional_comments_for_the_survey",
    "Issues_Questions_C_low_up_by_SunCulture",
    "Record_your_current_location",
    ##"background-audio",
    ##"__version__",
    ##"meta/audit",
    #"meta/instanceID",
    ##"meta/deprecatedID",
    "interviewed2023",
    "_xform_id_string",
    "_uuid",
    ##"_attachments",
    "_status",
    "_geolocation",
    "_submission_time",
    ##"_tags",
    ##"_notes",
    ##"_validation_status",
    "_submitted_by",
    ##"_supplementalDetails",
    }

def connect_to_kobotoolbox():
    """Establish connection to KoboToolbox API."""
    response = requests.get(KOBO_API_URL, headers=HEADERS)
    return response

def fetch_kobo_data():
    """Fetch KoboToolbox data and return only specified fields."""
    response = connect_to_kobotoolbox()
    
    if response.status_code == 200:
        try:
            data = response.json()
            if "results" in data:
                filtered_data = [
                    {key: value for key, value in entry.items() if key in INCLUDED_FIELDS}
                    for entry in data["results"]
                ]
                return filtered_data
            else:
                print("No results found in response.")
                return None
        except requests.exceptions.JSONDecodeError as e:
            print("JSON Decode Error:", e)
            return None
    else:
        print("Error:", response.status_code, response.text)
        return None

def connect_to_postgres():
    """Establishes a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(**ep_stage_db_params)
        print("✅ Connected to PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return None
    
def sanitize_column_name(field_name):
    """Replace special characters in field names to make them PostgreSQL-compatible."""
    return field_name.lower()

def save_to_postgres(data, table_name="carbon_2023_vintage"):
    conn = connect_to_postgres()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # Convert field names to lowercase and replace special characters
        sanitized_fields = [sanitize_column_name(field) for field in INCLUDED_FIELDS]
        
        # Define the insert query dynamically based on sanitized fields
        columns = ", ".join(sanitized_fields)
        values_placeholders = ", ".join(["%s"] * len(sanitized_fields))
        
        # Generate the update clause for all fields except _uuid
        update_clause = ", ".join(
            f"{col} = EXCLUDED.{col}" 
            for col in sanitized_fields 
            if col != "_uuid"
        )
        
        insert_query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({values_placeholders})
        ON CONFLICT (_uuid)
        DO UPDATE SET {update_clause};
        """
        
        total_affected = 0
        for entry in data:
            try:
                record = tuple(
                    None if isinstance(entry.get(field), (dict, list)) else entry.get(field, None)
                    for field in INCLUDED_FIELDS
                )
                cursor.execute(insert_query, record)
                total_affected += cursor.rowcount  # Count inserted or updated rows
            except Exception as e:
                error_uuid = entry.get("_uuid", "Unknown UUID")
                print(f"❌ Error processing record with _uuid: {error_uuid} - {str(e)}")
                print(f"Record data: {record}")
                raise

        conn.commit()
        print(f"✅ Successfully inserted/updated {total_affected} records out of {len(data)} fetched.")
    except Exception as e:
        print(f"❌ Error saving to PostgreSQL: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
def main():
    """Main execution function."""
    data = fetch_kobo_data()
    if data:
        print(f"✅ Retrieved {len(data)} records from KoboToolbox.")
        save_to_postgres(data)

if __name__ == "__main__":
    main()
