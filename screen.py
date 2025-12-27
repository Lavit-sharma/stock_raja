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

def git_push_screenshot(path):
    """Commits and pushes a single file to GitHub immediately."""
    try:
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", path], check=True)
        subprocess.run(["git", "commit", "-m", f"📸 Update: {path}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"🚀 Deployed {path} to GitHub.", flush=True)
    except Exception as e:
        print(f"⚠️ Git Push failed for {path}: {e}", flush=True)

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    
    # YOUR STEALTH OPTIONS (INTEGRATED)
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def inject_tv_cookies(driver):
    """Your proven cookie injection logic."""
    session_id = os.getenv("TRADINGVIEW_COOKIES")
    if not session_id:
        print("❌ Error: TRADINGVIEW_COOKIES secret missing!", flush=True)
        return False

    # Navigate to robots.txt first to set domain context
    driver.get("https://www.tradingview.com/robots.txt")
    time.sleep(2)
    
    try:
        driver.add_cookie({
            'name': 'sessionid',
            'value': session_id,
            'domain': '.tradingview.com',
            'path': '/',
            'secure': True,
            'httpOnly': True
        })
        driver.refresh()
        print("✅ TV Session Cookies Injected.", flush=True)
        return True
    except Exception as e:
        print(f"❌ Cookie Injection Failed: {e}", flush=True)
        return False

# ---------------- MAIN ---------------- #

def main():
    # 1. LOAD DATA (USING YOUR PROVEN PANDAS HEADER FIX)
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        
        # Load Strategy Data
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
        
        # Load Links
        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        
        link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                            df_stocks.iloc[:, 2].astype(str).str.strip()))
        
        print(f"✅ Connected. Scanning {len(df_mv2)} symbols.", flush=True)
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    # 2. LOOP THROUGH SYMBOLS
    for index, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        
        # Cleanup math values (G: dailychange, H: monthlychange)
        try:
            daily_raw = str(row.get('dailychange', '0')).replace('%', '').strip()
            monthly_raw = str(row.get('monthlychange', '0')).replace('%', '').strip()
            
            daily_val = float(daily_raw) if daily_raw else 0.0
            monthly_val = float(monthly_raw) if monthly_raw else 0.0
        except ValueError:
            continue

        # Strategy Logic
        is_daily_alert = daily_val >= 0.07
        is_monthly_alert = monthly_val >= 0.25

        if is_daily_alert or is_monthly_alert:
            url = link_map.get(symbol)
            if not url or not str(url).startswith('http'):
                continue

            print(f"🎯 Match: {symbol} (D: {daily_val}, M: {monthly_val})", flush=True)
            
            try:
                driver.get(url)
                # Wait for your proven XPath or chart container
                WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
                )
                time.sleep(5) # Let Indicators load

                # Get Chart Element for Clean Screenshot
                chart = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")

                # Handle Daily Alert
                if is_daily_alert:
                    # Switch to 1 Day (Optional typing)
                    webdriver.ActionChains(driver).send_keys("1D").send_keys(Keys.ENTER).perform()
                    time.sleep(3)
                    path = f"{DAILY_DIR}/{symbol}.png"
                    chart.screenshot(path)
                    git_push_screenshot(path)

                # Handle Monthly Alert
                if is_monthly_alert:
                    # Switch to 1 Month
                    webdriver.ActionChains(driver).send_keys("1M").send_keys(Keys.ENTER).perform()
                    time.sleep(3)
                    path = f"{MONTHLY_DIR}/{symbol}.png"
                    chart.screenshot(path)
                    git_push_screenshot(path)

            except Exception as e:
                print(f"⚠️ Failed {symbol}: {e}", flush=True)

    driver.quit()
    print("🏁 Processing Complete.", flush=True)

if __name__ == "__main__":
    main()
