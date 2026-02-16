import requests
import pandas as pd
import json
import os
from datetime import datetime

# ==============================
# CONFIG
# ==============================

GUMROAD_TOKEN = os.getenv("GUMROAD_TOKEN")
GUMROAD_URL = "https://api.gumroad.com/v2/sales"

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")

RAW_TABLE = "Gumroad_raw_sales"
CLEAN_TABLE = "Gumroad_clean_sales"

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}


# ==============================
# GET EXISTING ORDER IDS (Optimized)
# ==============================

def get_existing_order_ids(table_name):
    existing_ids = set()
    offset = None

    while True:
        url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"
        params = {"offset": offset} if offset else {}

        response = requests.get(url, headers=HEADERS, params=params)
        data = response.json()

        for record in data.get("records", []):
            order_id = record["fields"].get("Order ID")
            if order_id:
                existing_ids.add(str(order_id))

        offset = data.get("offset")
        if not offset:
            break

    return existing_ids


# ==============================
# FETCH SALES FROM GUMROAD
# ==============================

def fetch_sales():
    print("Token exists:", GUMROAD_TOKEN is not None)
    response = requests.get(
        GUMROAD_URL,
        params={
            "access_token": GUMROAD_TOKEN,
            "per_page": 100  # Fetch more per run
        }
    )
    print("Status Code:", response.status_code)
    print("Raw Response:", response.text)

    if response.status_code != 200:
        return []

    return response.json().get("sales", [])

   # data = response.json()

    #if not data.get("success"):
     #   print("❌ Gumroad API failed:", data)
      #  return []

    #return data.get("sales", [])


# ==============================
# INSERT INTO AIRTABLE
# ==============================

def insert_record(table_name, payload):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"
    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code not in [200, 201]:
        print("❌ Airtable Insert Failed:", response.text)


# ==============================
# MAIN ETL PIPELINE
# ==============================

def run_pipeline():

    print("🚀 Scheduled ETL Started at:", datetime.utcnow())

    sales = fetch_sales()

    if not sales:
        print("No sales found.")
        return

    # Load existing IDs once (FAST)
    raw_existing_ids = get_existing_order_ids(RAW_TABLE)
    clean_existing_ids = get_existing_order_ids(CLEAN_TABLE)

    inserted_raw = 0
    inserted_clean = 0
    skipped = 0

    for sale in sales:

        order_id = str(sale.get("order_id"))

        # ---------------------------
        # DUPLICATE CHECK
        # ---------------------------
        if order_id in raw_existing_ids:
            skipped += 1
            continue

        # ---------------------------
        # FORMAT DATE
        # ---------------------------
        created_at = pd.to_datetime(
            sale.get("created_at"),
            errors="coerce"
        ).strftime("%Y-%m-%dT%H:%M:%S")

        # ---------------------------
        # SAVE RAW
        # ---------------------------
        raw_payload = {
            "fields": {
                "Order ID": order_id,
                "Email": sale.get("email"),
                "Product Name": sale.get("product_name"),
                "Price": sale.get("price"),
                "Currency": sale.get("currency_symbol"),
                "Country": sale.get("country"),
                "State": sale.get("state"),
                "Refunded": sale.get("refunded"),
                "Purchase Date": created_at,
                "Raw JSON": json.dumps(sale)
            }
        }

        insert_record(RAW_TABLE, raw_payload)
        inserted_raw += 1
        raw_existing_ids.add(order_id)

        # ---------------------------
        # CLEAN DATA
        # ---------------------------
      
        clean_payload = {
            "fields": {
                "Order ID": order_id,
                "Email": sale.get("email") or "unknown@email.com",
                "Product Name": sale.get("product_name") or "Unknown Product",
                "Price": sale.get("price") or 0,
                "Currency": sale.get("currency_symbol") or "Unknown",
                "Country": sale.get("country") or "Unknown",
                "State": sale.get("state") or "Unknown",
                "Purchase Date": created_at
            }
        }

        if order_id not in clean_existing_ids:
            insert_record(CLEAN_TABLE, clean_payload)
            inserted_clean += 1
            clean_existing_ids.add(order_id)

    print("\n===== ETL SUMMARY =====")
    print("Raw Inserted:", inserted_raw)
    print("Clean Inserted:", inserted_clean)
    print("Skipped:", skipped)
    print("Completed at:", datetime.utcnow())


# ==============================
# EXECUTE
# ==============================

if __name__ == "__main__":
    run_pipeline()
