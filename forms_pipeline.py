import requests
import json
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone

# ==========================
# ENV CONFIG
# ==========================
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

RAW_TABLE = "Gumroad_raw_sales"
CLEAN_TABLE = "Gumroad_clean_sales"
GOOGLE_SHEET_NAME = "demo information (Responses)"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# ==========================
# GOOGLE SHEETS CONNECTION
# ==========================
if "GOOGLE_CREDENTIALS" in os.environ:
    google_creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
else:
    with open("forms-automation-488606-13ee1a0ed319.json") as f:
        google_creds_dict = json.load(f)

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_info(
    google_creds_dict,
    scopes=scope
)

client = gspread.authorize(credentials)
sheet = client.open(GOOGLE_SHEET_NAME).sheet1

# ==========================
# AIRTABLE HELPERS
# ==========================

def find_record_by_email(table_name, email):
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"

   
    params = {
        "filterByFormula": f"LOWER({{Email}})='{email}'"
    }


    print("Searching in table:", table_name)
    print("Filter formula:", f"{{Email id}}='{email}'")
    


    response = requests.get(url, headers=HEADERS, params=params)

    print("Airtable response:", response.text)

    if response.status_code != 200:
        print("Airtable fetch error:", response.text)
        return None

    data = response.json()

    if data.get("records"):
        return data["records"][0]["id"]

    return None
   


def update_record(table_name, record_id, fields):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}/{record_id}"

    response = requests.patch(url, headers=HEADERS, json={"fields": fields})

    if response.status_code != 200:
        print("Airtable update error:", response.text)
        return False

    return True


# ==========================
# MAIN PIPELINE
# ==========================

def main():
    print("Starting forms pipeline...")
    rows = sheet.get_all_records()
    print("Total rows found:", len(rows))

    rows = sheet.get_all_records()
    processed_col = sheet.find("PROCESSED").col

    for index, row in enumerate(rows):
       

        if str(row.get("Processed", "")).lower() == "yes":
            continue
       
        # Extract & lowercase everything
        name = str(row.get("NAME:", "")).strip().lower()
        email = str(row.get("EMAIL ID:", "")).strip().lower()
        profession = str(row.get("PROFESSION:", "")).strip().lower()
        phone = str(row.get("PHONE NUMBER:", "")).strip().lower()

        print("Form email:", email)
        print("Profession:", profession)
        print("Phone:", phone)

        if not email:
            continue

        # ==========================
        # RAW UPDATE
        # ==========================

        raw_record_id = find_record_by_email(RAW_TABLE, email)

        if raw_record_id:
            raw_fields = {
                "Name": name,
                "Email": email,
                "Profession": profession,
                "Phone No": phone,
            
            }

            update_record(RAW_TABLE, raw_record_id, raw_fields)
            print(f"Raw updated for {email}")
        else:
            print(f"No Raw record found for {email}")
            continue

        # ==========================
        # CLEAN UPDATE
        # ==========================

        clean_record_id = find_record_by_email(CLEAN_TABLE, email)

        if clean_record_id:
            clean_fields = {
                "Name": name,
                "Email": email,
                "Profession": profession,
                "Phone No": phone,
            
            }

            update_record(CLEAN_TABLE, clean_record_id, clean_fields)
            print(f"Clean updated for {email}")
        else:
            print(f"No Clean record found for {email}")
            continue

        # ==========================
        # MARK AS PROCESSED
        # ==========================

        sheet.update_cell(index + 2, processed_col, "Yes")
        print(f"Completed processing for {email}")


if __name__ == "__main__":
    main()