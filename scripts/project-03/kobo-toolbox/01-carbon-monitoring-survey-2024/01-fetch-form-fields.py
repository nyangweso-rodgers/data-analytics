import os
from dotenv import load_dotenv
import requests

# Load environment variables from .env file
load_dotenv()

# KoboToolbox API details
KOBO_API_URL = os.getenv("Carbon_2nd_Monitoring_Survey_2024")
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

if __name__ == "__main__":
    form_fields = fetch_form_fields()
    if form_fields:
        print("Form Fields:", form_fields)