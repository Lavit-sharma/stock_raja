import os, time, json, gspread, subprocess
import pandas as pd
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

def git_push_screenshot(path):
    try:
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "add", path], check=True)
        subprocess.run(["git", "commit", "-m", f"📸 Update: {path}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"🚀 Deployed {path}", flush=True)
    except:
        pass

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Stealth settings to prevent cookie rejection
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    """The key fix: Visit domain, then inject secret."""
    session_id = os.getenv("TRADINGVIEW_COOKIES")
    if not session_id:
        print("❌ Error: TRADINGVIEW_COOKIES secret missing in GitHub!", flush=True)
        return False

    try:
        # 1. First visit to establish domain
        driver.get("https://www.tradingview.com/robots.txt")
        time.sleep(3)
        
        # 2. Add the cookie with specific TradingView parameters
        driver.add_cookie({
            'name': 'sessionid',
            'value': session_id.strip(),
            'domain': '.tradingview.com',
            'path': '/',
            'secure': True
        })
        
        # 3. Refresh to activate session
        driver.get("https://www.tradingview.com/")
        time.sleep(5)
        print("✅ Cookie Injected. Session active.", flush=True)
        return True
    except Exception as e:
        print(f"❌ Cookie Injection Failed: {e}", flush=True)
        return False

def main():
    # Load Sheets
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    client = gspread.service_account_from_dict(json.loads(creds_json))
    
    mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
    df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
    
    stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
    df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
    link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                        df_stocks.iloc[:, 2].astype(str).str.strip()))

    driver = get_driver()
    
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    print(f"📊 Scanning {len(df_mv2)} symbols...", flush=True)

    for index, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        try:
            daily = float(str(row.get('dailychange', '0')).replace('%', '').strip() or 0)
            monthly = float(str(row.get('monthlychange', '0')).replace('%', '').strip() or 0)
        except: continue

        if daily >= 0.07 or monthly >= 0.25:
            url = link_map.get(symbol)
            if not url or "tradingview.com" not in url: continue

            driver.get(url)
            time.sleep(10) # Longer wait for your indicators to load

            try:
                # Screenshot logic
                chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                
                if daily >= 0.07:
                    webdriver.ActionChains(driver).send_keys("1D").send_keys(Keys.ENTER).perform()
                    time.sleep(5)
                    path = f"{DAILY_DIR}/{symbol}.png"
                    chart.screenshot(path)
                    git_push_screenshot(path)

                if monthly >= 0.25:
                    webdriver.ActionChains(driver).send_keys("1M").send_keys(Keys.ENTER).perform()
                    time.sleep(5)
                    path = f"{MONTHLY_DIR}/{symbol}.png"
                    chart.screenshot(path)
                    git_push_screenshot(path)
            except Exception as e:
                print(f"⚠️ Error capturing {symbol}: {e}", flush=True)

    driver.quit()

if __name__ == "__main__":
    main()
