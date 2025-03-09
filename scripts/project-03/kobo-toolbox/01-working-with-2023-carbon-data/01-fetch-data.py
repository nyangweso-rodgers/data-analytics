import requests
import csv

# KoboToolbox API details
KOBO_API_URL = "https://kf.kobotoolbox.org/api/v2/assets/aNo2GBnQSU8rghPQGmqTt9/data/?format=json"
KOBO_API_TOKEN = "5f79b2c3f7a21850f931313b2ff40672792a89e2"
HEADERS = {
    "Authorization": f"Token {KOBO_API_TOKEN}",
    "Accept": "application/json"
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
    #"Take_photo_of_farmer_e_farmer_for_consent",
    "Additional_comments_for_the_survey",
    "Issues_Questions_C_low_up_by_SunCulture",
    "Record_your_current_location",
    #"background-audio",
    #"__version__",
    #"meta/audit",
    "meta/instanceID",
    #"meta/deprecatedID",
    "interviewed2023",
    "_xform_id_string",
    "_uuid",
    #"_attachments",
    "_status",
    "_geolocation",
    "_submission_time",
    #"_tags",
    #"_notes",
    #"_validation_status",
    "_submitted_by",
    #"_supplementalDetails",
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
def save_to_csv(data, filename="kobo_data.csv"):
    """Save the filtered KoboToolbox data to a CSV file."""
    if not data:
        print("No data to save.")
        return

    # Extract field names from the first dictionary in the list
    fieldnames = data[0].keys() if data else []
    
    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
def main():
    """Main function to execute the script."""
    data = fetch_kobo_data()
    if data:
        print("Filtered Data:", data)
        save_to_csv(data)

if __name__ == "__main__":
    main()
