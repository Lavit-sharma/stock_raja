import os
import json
import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound

# Load credentials from environment
creds_json = os.environ.get("GSPREAD_CREDENTIALS")
if not creds_json:
    raise Exception("GSPREAD_CREDENTIALS environment variable not set")

creds_dict = json.loads(creds_json)
gc = gspread.service_account_from_dict(creds_dict)

spreadsheet_name = 'Tradingview Data Reel Experimental May'
worksheet_name = 'Sheet5'

try:
    sheet = gc.open(spreadsheet_name)
except SpreadsheetNotFound:
    print(f"Error: Spreadsheet '{spreadsheet_name}' not found or service account does not have access.")
    print("Make sure you shared the spreadsheet with your service account email.")
    exit(1)

try:
    worksheet = sheet.worksheet(worksheet_name)
except WorksheetNotFound:
    print(f"Error: Worksheet '{worksheet_name}' not found in spreadsheet '{spreadsheet_name}'.")
    print("Check the worksheet name and capitalization.")
    exit(1)

print(f"Successfully accessed worksheet '{worksheet_name}' in spreadsheet '{spreadsheet_name}'!")

# Now you can continue scraping or reading/writing data
# Example: read all values
data = worksheet.get_all_values()
print(f"Number of rows fetched: {len(data)}")
