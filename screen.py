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
        print(f"⚠️ Git Push failed: {e}", flush=True)

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    
    # YOUR PROVEN STEALTH OPTIONS
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.experimental_options["excludeSwitches"] = ["enable-automation"]
    opts.experimental_options["useAutomationExtension"] = False
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    # Stealth defineProperty
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def inject_tv_cookies(driver):
    """Injects cookies using the logic from your provided code."""
    # This checks for a local file OR the GitHub Secret environment variable
    session_id = os.getenv("TRADINGVIEW_COOKIES")
    
    # We must visit the domain first
    driver.get("https://www.tradingview.com/robots.txt")
    time.sleep(2)

    try:
        if session_id:
            # Handle if the secret is just the ID string
            driver.add_cookie({
                "name": "sessionid",
                "value": session_id.strip(),
                "domain": ".tradingview.com",
                "path": "/"
            })
            print("✅ sessionid cookie injected from Secrets.", flush=True)
        elif os.path.exists("cookies.json"):
            # Handle if you uploaded a cookies.json file
            with open("cookies.json", "r") as f:
                for c in json.load(f):
                    try:
                        driver.add_cookie({
                            "name": c.get("name"),
                            "value": c.get("value"),
                            "domain": c.get("domain", ".tradingview.com"),
                            "path": c.get("path", "/")
                        })
                    except: pass
            print("✅ Cookies injected from cookies.json.", flush=True)
        
        driver.refresh()
        time.sleep(3)
        return True
    except Exception as e:
        print(f"❌ Cookie Injection Failed: {e}", flush=True)
        return False

# ---------------- MAIN ---------------- #

def main():
    # 1. GOOGLE SHEETS AUTH
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        
        # Strategy Data (Daily/Monthly changes)
        mv2_sheet = client.open_by_url(MV2_SQL_URL).sheet1
        mv2_raw = mv2_sheet.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
        
        # Stock Links
        stock_sheet = client.open_by_url(STOCK_LIST_URL).sheet1
        stock_raw = stock_sheet.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        
        link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                            df_stocks.iloc[:, 2].astype(str).str.strip()))
        
        print(f"✅ Connected to Sheets. Scanning {len(df_mv2)} symbols.", flush=True)
    except Exception as e:
        print(f"❌ Sheet Connection Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("⚠️ Proceeding without login...")

    # 2. LOOP AND SCREENSHOT
    for index, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        
        try:
            # Cleanup values for math
            d_str = str(row.get('dailychange', '0')).replace('%', '').strip()
            m_str = str(row.get('monthlychange', '0')).replace('%', '').strip()
            
            daily_val = float(d_str) if d_str else 0.0
            monthly_val = float(m_str) if m_str else 0.0
        except:
            continue

        is_daily = daily_val >= 0.07
        is_monthly = monthly_val >= 0.25

        if is_daily or is_monthly:
            url = link_map.get(symbol)
            if not url or not str(url).startswith('http'):
                continue

            print(f"🔍 Processing: {symbol} (D:{daily_val} | M:{monthly_val})", flush=True)
            
            try:
                driver.get(url)
                # Wait for chart to appear
                WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
                )
                time.sleep(5) # Allow indicators to load

                chart_el = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")

                if is_daily:
                    # Switch to Daily timeframe
                    webdriver.ActionChains(driver).send_keys("1D").send_keys(Keys.ENTER).perform()
                    time.sleep(4)
                    path = f"{DAILY_DIR}/{symbol}.png"
                    chart_el.screenshot(path)
                    git_push_screenshot(path)

                if is_monthly:
                    # Switch to Monthly timeframe
                    webdriver.ActionChains(driver).send_keys("1M").send_keys(Keys.ENTER).perform()
                    time.sleep(4)
                    path = f"{MONTHLY_DIR}/{symbol}.png"
                    chart_el.screenshot(path)
                    git_push_screenshot(path)

            except Exception as e:
                print(f"⚠️ Error on {symbol}: {e}", flush=True)

    driver.quit()
    print("🏁 Processing Complete.", flush=True)

if __name__ == "__main__":
    main()
