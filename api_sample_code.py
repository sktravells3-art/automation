import requests
import pandas as pd
import gspread
import os
import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

def get_and_dump_data():
    # --- AUTHENTICATION ---
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    creds_json = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
    if not creds_json:
        raise ValueError("GCP_SERVICE_ACCOUNT_KEY not found in environment variables")
    
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)

    # --- API FETCH ---
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': 50,
        'page': 1,
        'sparkline': 'false'
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df = df[['name', 'symbol', 'current_price', 'market_cap', 'total_volume', 'last_updated']]

        # --- GOOGLE SHEETS ---
        spreadsheet_id = "1WF4ibJp6z-3VuFupw_AhMNKI_P7LlpLr-Q123HEaJt4"
        spreadsheet = gc.open_by_key(spreadsheet_id)

        # --- MAIN DATA SHEET (DDS) ---
        try:
            worksheet = spreadsheet.worksheet("DDS")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="DDS", rows="1000", cols="20")

        worksheet.clear()
        set_with_dataframe(worksheet, df)

        # --- DUMP LOGS SHEET ---
        try:
            log_sheet = spreadsheet.worksheet("DUMP Logs")
        except gspread.exceptions.WorksheetNotFound:
            log_sheet = spreadsheet.add_worksheet(title="DUMP Logs", rows="1000", cols="5")

        # Get column A values
        col_a = log_sheet.col_values(1)

        # Add header if sheet is empty
        if not col_a:
            log_sheet.update("A1", [["Timestamp"]])
            next_row = 2
        else:
            next_row = len(col_a) + 1

        # Current timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Write timestamp
        log_sheet.update(f"A{next_row}", [[current_time]])

        print("✅ Success! API data + timestamp logged.")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    get_and_dump_data()
