import os
import time
import json
import gspread
import pandas as pd
import mysql.connector

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843
MV2_SQL_URL = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

TARGET_TABLE = "filter"

# Updated to columns V and W headers
TRIGGER_COLUMNS = ["D_Trigger", "W_Trigger"] 

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_str(v):
    return str(v).strip() if v else ""

def safe_int(v):
    """Converts strings/floats to int. Returns -1 for empty or non-numeric strings."""
    try:
        val = str(v).strip()
        if not val: return -1
        return int(float(val))
    except (ValueError, TypeError):
        return -1

# ---------------- DB CLASS ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        if self.conn:
            try: self.conn.close()
            except: pass
        self.conn = mysql.connector.connect(**self.config)
        self.conn.autocommit = True
        return self.conn

    def ensure(self):
        if not self.conn or not self.conn.is_connected():
            return self.connect()
        return self.conn

    def close(self):
        try:
            if self.conn: self.conn.close()
        except: pass

def save_screenshot(db: DB, symbol, timeframe, filter_type, image):
    query = f"""
        INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, filter_type, screenshot)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            created_at = CURRENT_TIMESTAMP
    """
    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, (symbol, timeframe, filter_type, image))
            cur.close()
            log(f"✅ Saved: {symbol} | {filter_type} | {timeframe}")
            return
        except Exception as e:
            log(f"⚠️ DB error: {e}")
            db.connect()
            time.sleep(1)

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        for c in cookies:
            driver.add_cookie({"name": c.get("name"), "value": c.get("value"), "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        return True
    except: return False

# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None
    try:
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        # 1. Fetch Triggers (MV2 Sheet)
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        # 2. Fetch URLs (Stock List Sheet)
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        week_urls = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 2].str.strip()))
        day_urls = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 3].str.strip()))

        driver = get_driver()
        if not inject_tv_cookies(driver): 
            log("❌ Cookie injection failed.")
            return

        # 3. Process D_Trigger and W_Trigger
        for filter_col in TRIGGER_COLUMNS:
            if filter_col not in df_mv2.columns:
                log(f"⚠️ Header '{filter_col}' not found in Google Sheet.")
                continue

            log(f"🔍 Scanning {filter_col} for value: 0")
            
            # Logic: Filter rows where the value evaluates to 0
            triggered_rows = df_mv2[df_mv2[filter_col].apply(safe_int) == 0]

            for _, row in triggered_rows.iterrows():
                symbol = safe_str(row.iloc[0])
                if not symbol: continue

                log(f"🚀 Triggered: {symbol} (Reason: 0 in {filter_col})")

                tasks = [("day", day_urls.get(symbol)), ("week", week_urls.get(symbol))]
                
                for tf_name, url in tasks:
                    if url and "tradingview.com" in url:
                        try:
                            driver.get(url)
                            chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                            )
                            time.sleep(POST_LOAD_SLEEP)
                            save_screenshot(db, symbol, tf_name, filter_col, chart.screenshot_as_png)
                        except Exception as e:
                            log(f"❌ Screenshot failed for {symbol} {tf_name}: {e}")

        log("🏁 All triggers processed.")
    finally:
        if driver: driver.quit()
        db.close()

if __name__ == "__main__":
    main()
