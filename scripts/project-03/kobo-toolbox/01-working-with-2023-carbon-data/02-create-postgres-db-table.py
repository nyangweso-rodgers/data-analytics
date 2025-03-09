import os
import psycopg2

# Database connection parameters
ep_stage_db_params = {
    "dbname": os.getenv("ep_stage_db"),
    "user": os.getenv("ep_stage_db_user"),
    "password": os.getenv("ep_stage_db_password"),
    "host": os.getenv("ep_stage_db_host"),
    "port": os.getenv("ep_stage_db_port")
}

# Define the table creation SQL statement
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS carbon_2023_vintage (
     _uuid VARCHAR(36) PRIMARY KEY,
    date DATE,
    Name_of_surveyor TEXT,
    Farmer_consented_to_the_interview TEXT,
    Customer_ID TEXT,
    Customer_Name TEXT,
    Customer_SunCulture_Region TEXT,
    Customer_County TEXT,
    Customer_s_address_Village TEXT,
    Age_of_customer DOUBLE PRECISION,
    Gender_of_customer TEXT,
    Respondent_Name TEXT,
    Respondent_s_telephone_number TEXT,
    Alternative_Phone_Number TEXT,
    Model_of_SunCulture_pump_being_used TEXT,
    SunCulture_Pump_date_and_year_of_purchase TEXT,
    Satus_of_SunCulture_Solar_pump TEXT,
    Did_you_previously_own_a_diese BOOLEAN,
    How_much_money_is_sp_mp_monthly_average DOUBLE PRECISION,
    Total_yearly_yield_b_sing_Sunculture_pump DOUBLE PRECISION,
    Total_yearly_yield_n_sing_SunCulture_pump DOUBLE PRECISION,
    Time_spent_on_field_sing_Sunculture_pump DOUBLE PRECISION,
    Time_spent_on_field_sing_Sunculture_pump_001 DOUBLE PRECISION,
    No_of_women_involve_sing_SunCulture_pump DOUBLE PRECISION,
    No_of_women_involve_sing_SunCulture_pump_001 DOUBLE PRECISION,
    DaysDrySeason DOUBLE PRECISION,
    How_many_hours_per_day_do_you_ DOUBLE PRECISION,
    DaysRainySeason DOUBLE PRECISION,
    How_many_hours_per_day_do_you__001 DOUBLE PRECISION,
    How_much_harvest_was_e_of_Sunculture_pump DOUBLE PRECISION,
    How_much_harvest_is_sing_SunCulture_pump DOUBLE PRECISION,
    Did_you_use_sprinkle_the_pumptype_pump TEXT,
    Additional_comments_for_the_survey TEXT,
    Issues_Questions_C_low_up_by_SunCulture TEXT,
    Record_your_current_location JSONB,
    meta_instanceID TEXT,
    interviewed2023 BOOLEAN,
    _xform_id_string TEXT,
    _status TEXT,
    _geolocation JSONB,
    _submission_time TIMESTAMP,
    _submitted_by TEXT
);
"""

def connect_to_postgres():
    """Establishes a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(**ep_stage_db_params)
        print("✅ Connected to PostgreSQL!")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

def create_table():
    """Creates the Kobo data table if it does not exist."""
    conn = connect_to_postgres()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("✅ Table created (if not exists).")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_table()
