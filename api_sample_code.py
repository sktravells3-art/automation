import requests
import pandas as pd
import gspread
import os
import json
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

def get_and_dump_data():
    # --- AUTHENTICATION ---
    # We will get the credentials from a GitHub Secret later
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    
    # This reads the secret environment variable we will set in GitHub
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

        # --- GOOGLE SHEETS DUMP ---
        spreadsheet_id = "1RZ1_bAxhv4Nannl1v5DsY8Dpu6JmAYY2eC95kUHUeSI"
        spreadsheet = gc.open_by_key(spreadsheet_id)

        try:
            worksheet = spreadsheet.worksheet("DDS")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="DDS", rows="1000", cols="20")

        worksheet.clear()
        set_with_dataframe(worksheet, df)
        print("✅ Success! API data synced via GitHub Action.")

    except Exception as e:
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    get_and_dump_data()
