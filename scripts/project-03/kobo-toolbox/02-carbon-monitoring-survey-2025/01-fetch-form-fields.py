import os
from dotenv import load_dotenv
import requests
import csv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# KoboToolbox API details
KOBO_API_URL = os.getenv("Carbon_3rd_Monitoring_Survey_2025")
KOBO_API_TOKEN = os.getenv("KOBO_API_TOKEN")
HEADERS = {
    "Authorization": f"Token {KOBO_API_TOKEN}",
    "Accept": "application/json"
}
def connect_to_kobotoolbox():
    """Establish connection to KoboToolbox API."""
    response = requests.get(KOBO_API_URL, headers=HEADERS)
    return response

# fetch Kobotoolbox form fields
def fetch_form_fields():
    """Fetch specific form fields from KoboToolbox and count them."""
    response = connect_to_kobotoolbox()
    
    if response.status_code == 200:
        try:
            data = response.json()
            if "results" in data and len(data["results"]) > 0:
                # Extract the keys (field names) from the first submission
                form_fields = data["results"][0].keys()
                field_count = len(form_fields)  # Count the number of fields
                print(f"Number of fields: {field_count}")
                return list(form_fields)
            else:
                print("No results found in response.")
                return None
        except requests.exceptions.JSONDecodeError as e:
            print("JSON Decode Error:", e)
            return None
    else:
        print("Error:", response.status_code, response.text)
        return None

def save_form_fields_to_csv(fields, filename="kobo_form_fields.csv"):
    """Save the form fields to a CSV file."""
    if not fields:
        print("No fields to save.")
        return
    
    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["Field Name"])  # Write header
            writer.writerows([[field] for field in fields])  # Write each field as a row
        print(f"Form fields successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def save_form_fields_to_excel(fields, filename="form-fields-data-v1.xlsx"):
    """Save the form fields to an Excel file."""
    if not fields:
        print("No fields to save.")
        return
    
    try:
        # Create a DataFrame with the fields
        df = pd.DataFrame(fields, columns=["Field Name"])
        
        # save to Excel file
        df.to_excel(filename, index=True)
        print(f"Form fields successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving to Excel: {e}")

def main():
    form_fields = fetch_form_fields()
    if form_fields:
        print("Form Fields:", form_fields)
        #save_fields_to_csv(form_fields)
        save_form_fields_to_excel(form_fields)
    
if __name__ == "__main__":
    main()