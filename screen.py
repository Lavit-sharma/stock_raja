import os, time, json, gspread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
# The URLs for your two separate spreadsheets
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit?ouid=101450253683874914240&usp=sheets_home&ths=true"

SCREENSHOT_DIR = "screenshots"
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# ---------------- GOOGLE SHEETS AUTH ---------------- #
def get_sheets_data():
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if creds_json:
            client = gspread.service_account_from_dict(json.loads(creds_json))
        else:
            client = gspread.service_account(filename="credentials.json")
            
        # Open separate spreadsheets
        sheet_stock_list = client.open_by_url(STOCK_LIST_URL).sheet1
        sheet_mv2_sql    = client.open_by_url(MV2_SQL_URL).sheet1
        
        return sheet_mv2_sql.get_all_records(), sheet_stock_list.get_all_records()
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        return [], []

# ---------------- SELENIUM SETUP ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new") # Required for GitHub Actions
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

# ---------------- MAIN EXECUTION ---------------- #
def main():
    data_rows, link_rows = get_sheets_data()
    if not data_rows: return

    # Map Symbol to Link for fast lookup
    link_map = {str(r['Symbol']).strip(): r['Link'] for r in link_rows if 'Link' in r}
    
    driver = get_driver()
    
    for row in data_rows:
        symbol = str(row.get('Symbol', '')).strip()
        
        # Clean numeric data (handle strings like "0.08" or "8%")
        try:
            daily_val = float(str(row.get('daily change', 0)).replace('%', ''))
            monthly_val = float(str(row.get('monthlychange', 0)).replace('%', ''))
        except: continue

        # Apply Conditions
        is_daily = daily_val >= 0.07
        is_monthly = monthly_val >= 0.25

        if is_daily or is_monthly:
            url = link_map.get(symbol)
            if not url: continue

            print(f"⚡ Processing {symbol}...")
            driver.get(url)
            time.sleep(6) # Allow chart to load fully

            if is_daily:
                driver.save_screenshot(f"{SCREENSHOT_DIR}/{symbol}_DAILY.png")
            if is_monthly:
                # If your chart needs a click for monthly, add driver.find_element logic here
                driver.save_screenshot(f"{SCREENSHOT_DIR}/{symbol}_MONTHLY.png")

    driver.quit()

if __name__ == "__main__":
    main()
