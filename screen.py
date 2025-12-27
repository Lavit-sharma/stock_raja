import os, time, json, gspread, subprocess
import pandas as pd
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

DAILY_DIR = "screenshots/daily"
MONTHLY_DIR = "screenshots/monthly"
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(MONTHLY_DIR, exist_ok=True)

# ---------------- HELPERS ---------------- #

def git_push_file(path):
    """Instantly pushes the captured screenshot to GitHub."""
    try:
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", path], check=True)
        subprocess.run(["git", "commit", "-m", f"📸 Update: {path}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"🚀 Deployed {path} to GitHub.", flush=True)
    except Exception as e:
        print(f"⚠️ Git Push failed: {e}", flush=True)

def inject_cookies(driver):
    """Bypasses login using session cookies."""
    session_id = os.getenv("TV_COOKIES")
    if not session_id:
        print("❌ Error: TV_COOKIES secret is missing!", flush=True)
        return
    driver.get("https://www.tradingview.com/")
    driver.add_cookie({'name': 'sessionid', 'value': session_id, 'domain': '.tradingview.com', 'path': '/'})
    driver.refresh()
    print("✅ Successfully logged in via Cookies.", flush=True)

def change_timeframe(driver, interval):
    """Changes chart timeframe by typing shortcut keys (e.g., '1D', '1M')."""
    try:
        actions = webdriver.ActionChains(driver)
        actions.send_keys(interval).send_keys(Keys.ENTER).perform()
        time.sleep(5) # Wait for chart to refresh
    except Exception as e:
        print(f"⚠️ Could not change timeframe to {interval}: {e}", flush=True)

# ---------------- MAIN ---------------- #

def main():
    # 1. AUTH & DATA
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json))
    
    df_mv2 = pd.DataFrame(client.open_by_url(MV2_SQL_URL).sheet1.get_all_values())
    df_mv2.columns = df_mv2.iloc[0]; df_mv2 = df_mv2[1:] # Handle Headers
    
    df_stocks = pd.DataFrame(client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values())
    df_stocks.columns = df_stocks.iloc[0]; df_stocks = df_stocks[1:]
    
    link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                        df_stocks.iloc[:, 2].astype(str).str.strip()))

    # 2. BROWSER SETUP
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    inject_cookies(driver)

    # 3. LOOP THROUGH DATA
    print(f"📊 Scanning {len(df_mv2)} rows...", flush=True)
    
    for _, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        try:
            daily = float(str(row.get('dailychange', '0')).replace('%', '').strip() or 0)
            monthly = float(str(row.get('monthlychange', '0')).replace('%', '').strip() or 0)
        except: continue

        is_daily = daily >= 0.07
        is_monthly = monthly >= 0.25

        if is_daily or is_monthly:
            url = link_map.get(symbol)
            if not url or not str(url).startswith('http'): continue

            driver.get(url)
            time.sleep(8) # Load indicators

            # Isolate Chart Element
            chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))

            if is_daily:
                change_timeframe(driver, "1D")
                path = f"{DAILY_DIR}/{symbol}.png"
                chart.screenshot(path)
                git_push_file(path)

            if is_monthly:
                change_timeframe(driver, "1M")
                path = f"{MONTHLY_DIR}/{symbol}.png"
                chart.screenshot(path)
                git_push_file(path)

    driver.quit()
    print("🏁 All Tasks Complete.", flush=True)

if __name__ == "__main__":
    main()
