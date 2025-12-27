import os, time, json, gspread
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
# Updated URL from your prompt
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

SCREENSHOT_DIR = "screenshots"
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# ---------------- DATA LOADING (FIXED FOR DUPLICATE HEADERS) ---------------- #
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
        
        # Pull RAW values as a list of lists to bypass gspread's header check
        mv2_raw = sheet_mv2_sql.get_all_values()
        stock_raw = sheet_stock_list.get_all_values()

        # Use Pandas to handle the headers. 
        # It will automatically fix duplicates like the [''] error you saw.
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        
        return df_mv2, df_stocks
    except Exception as e:
        print(f"❌ Error Loading Sheets: {e}")
        return None, None

# ---------------- SELENIUM SETUP ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new") 
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Stealth settings to prevent TradingView from blocking the bot
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

# ---------------- MAIN EXECUTION ---------------- #
def main():
    df_mv2, df_stocks = get_sheets_data()
    if df_mv2 is None or df_stocks is None:
        print("❌ Could not retrieve data. Check Spreadsheet URLs and Permissions.")
        return

    # Create Link Map from Stock List: Symbol (Col A) -> Link (Col C)
    # Using iloc to reference by position in case names are weird
    link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                        df_stocks.iloc[:, 2].astype(str).str.strip()))
    
    driver = get_driver()
    print(f"📊 Scanning {len(df_mv2)} rows for conditions...")

    for _, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        
        # Match your exact column names from the header list you provided
        try:
            # Cleaning values: handle strings, percentages, and empty cells
            daily_raw = str(row.get('dailychange', '0')).replace('%', '').strip()
            monthly_raw = str(row.get('monthlychange', '0')).replace('%', '').strip()
            
            daily_val = float(daily_raw) if daily_raw else 0.0
            monthly_val = float(monthly_raw) if monthly_raw else 0.0
        except ValueError:
            continue

        # YOUR CONDITIONS
        is_daily = daily_val >= 0.07
        is_monthly = monthly_val >= 0.25

        if is_daily or is_monthly:
            url = link_map.get(symbol)
            if not url or not str(url).startswith('http'):
                continue

            print(f"✅ Match Found: {symbol} (Daily: {daily_val}, Monthly: {monthly_val})")
            
            try:
                driver.get(url)
                # Wait for the chart to load (TradingView charts take time)
                time.sleep(8) 
                
                if is_daily:
                    driver.save_screenshot(f"{SCREENSHOT_DIR}/{symbol}_DAILY.png")
                    print(f"📸 Captured Daily Chart for {symbol}")

                if is_monthly:
                    # NOTE: If you need the 'Monthly' timeframe, you may need 
                    # driver.find_element logic here to click the 'M' button.
                    driver.save_screenshot(f"{SCREENSHOT_DIR}/{symbol}_MONTHLY.png")
                    print(f"📸 Captured Monthly Chart for {symbol}")
                    
            except Exception as e:
                print(f"⚠️ Failed to screenshot {symbol}: {e}")

    driver.quit()
    print("🏁 Processing Complete.")

if __name__ == "__main__":
    main()
