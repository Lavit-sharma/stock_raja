import os, time, json, gspread
import pandas as pd
import mysql.connector
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

# MySQL Config from Env Vars
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 30
}

# Temp directories for processing
DAILY_DIR = "temp_daily"
MONTHLY_DIR = "temp_monthly"
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(MONTHLY_DIR, exist_ok=True)

# ---------------- HELPERS ---------------- #

def save_to_mysql(symbol, timeframe, image_path):
    """Inserts the screenshot into MySQL as a LONGBLOB."""
    try:
        with open(image_path, 'rb') as file:
            binary_data = file.read()

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Table must have columns: symbol (varchat), timeframe (varchar), screenshot (longblob)
        query = "INSERT INTO stock_screenshots (symbol, timeframe, screenshot) VALUES (%s, %s, %s)"
        cursor.execute(query, (symbol, timeframe, binary_data))
        
        conn.commit()
        print(f"🗄️ [DB] Saved {symbol} {timeframe} screenshot.", flush=True)
    except Exception as e:
        print(f"❌ DB Error: {e}", flush=True)
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver

def inject_tv_cookies(driver):
    print("🔑 Injected Cookies...", flush=True)
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        for c in cookies:
            try: driver.add_cookie(c)
            except: pass
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

# ---------------- MAIN ---------------- #

def main():
    # Load Google Sheets
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        client = gspread.service_account_from_dict(creds)
        
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
        
        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        link_map = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 2].str.strip()))
    except Exception as e:
        print(f"❌ Setup Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ Login Failed")
        driver.quit()
        return

    count = 0
    for _, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        try:
            daily = float(str(row.get('dailychange', '0')).replace('%', '').strip() or 0)
            monthly = float(str(row.get('monthlychange', '0')).replace('%', '').strip() or 0)
        except: continue

        if daily >= 0.07 or monthly >= 0.25:
            url = link_map.get(symbol)
            if not url: continue
            
            driver.get(url)
            try:
                chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CLASS_NAME, "chart-container-border")))
                time.sleep(8)

                if daily >= 0.07:
                    webdriver.ActionChains(driver).send_keys("1D").send_keys(Keys.ENTER).perform()
                    time.sleep(4)
                    path = f"{DAILY_DIR}/{symbol}.png"
                    chart.screenshot(path)
                    save_to_mysql(symbol, "daily", path)
                    count += 1

                if monthly >= 0.25:
                    webdriver.ActionChains(driver).send_keys("1M").send_keys(Keys.ENTER).perform()
                    time.sleep(4)
                    path = f"{MONTHLY_DIR}/{symbol}.png"
                    chart.screenshot(path)
                    save_to_mysql(symbol, "monthly", path)
                    count += 1
            except Exception as e:
                print(f"⚠️ Error {symbol}: {e}")

    driver.quit()
    print(f"🏁 Finished. {count} records added.")

if __name__ == "__main__":
    main()
