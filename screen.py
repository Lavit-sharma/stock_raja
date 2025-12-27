import os, time, json, gspread, subprocess
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

SCREENSHOT_DIR = "screenshots"
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

def git_push_screenshot(filename):
    """Commits and pushes a single file to GitHub immediately."""
    try:
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", filename], check=True)
        subprocess.run(["git", "commit", "-m", f"📸 Added screenshot: {filename}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"🚀 Successfully deployed {filename} to GitHub.", flush=True)
    except Exception as e:
        print(f"⚠️ Git Push Failed for {filename}: {e}", flush=True)

def get_sheets_data():
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        sheet_stock_list = client.open_by_url(STOCK_LIST_URL).sheet1
        sheet_mv2_sql    = client.open_by_url(MV2_SQL_URL).sheet1
        mv2_raw = sheet_mv2_sql.get_all_values()
        stock_raw = sheet_stock_list.get_all_values()
        return pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0]), pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
    except Exception as e:
        print(f"❌ Error Loading Sheets: {e}", flush=True)
        return None, None

def get_driver():
    opts = Options()
    # During testing, we use 'headless=new' which is the closest to a real browser
    opts.add_argument("--headless=new") 
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def main():
    df_mv2, df_stocks = get_sheets_data()
    if df_mv2 is None: return

    link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                        df_stocks.iloc[:, 2].astype(str).str.strip()))
    
    driver = get_driver()
    print(f"📊 Analyzing {len(df_mv2)} rows...", flush=True)

    for index, row in df_mv2.iterrows():
        if index % 50 == 0: print(f"⏳ Progress: Row {index}", flush=True)
        
        symbol = str(row.get('Symbol', '')).strip()
        try:
            daily_val = float(str(row.get('dailychange', '0')).replace('%', '').strip() or 0)
            monthly_val = float(str(row.get('monthlychange', '0')).replace('%', '').strip() or 0)
        except: continue

        if daily_val >= 0.07 or monthly_val >= 0.25:
            url = link_map.get(symbol)
            if not url or not str(url).startswith('http'): continue

            print(f"✅ Match: {symbol}. Opening Browser...", flush=True)
            try:
                driver.get(url)
                time.sleep(10) # Longer wait for testing
                
                path = f"{SCREENSHOT_DIR}/{symbol}.png"
                driver.save_screenshot(path)
                print(f"📸 Screenshot saved locally: {path}", flush=True)
                
                # DEPLOY IMMEDIATELY
                git_push_screenshot(path)
                
            except Exception as e:
                print(f"⚠️ Error: {e}", flush=True)

    driver.quit()

if __name__ == "__main__":
    main()
