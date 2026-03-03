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

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
PAGE_RETRY = 2

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_str(v):
    return str(v).strip() if v else ""

def safe_int(v):
    try:
        return int(float(str(v).strip()))
    except:
        return 0

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

def save_screenshot(db: DB, symbol, timeframe, image):
    query = f"""
        INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, screenshot)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            created_at = CURRENT_TIMESTAMP
    """
    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, (symbol, timeframe, image))
            cur.close()
            log(f"✅ Saved Screenshot: {symbol} [{timeframe}]")
            return
        except Exception as e:
            log(f"⚠️ DB error attempt {attempt+1}: {e}")
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
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        for c in cookies:
            driver.add_cookie({
                "name": c.get("name"), "value": c.get("value"),
                "domain": ".tradingview.com", "path": "/"
            })
        driver.refresh()
        return True
    except: return False

# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    try:
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        # 1. Fetch Triggers
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        # 2. Fetch URL Maps
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        week_urls = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 2].str.strip()))
        day_urls = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 3].str.strip()))

        driver = get_driver()
        if not inject_tv_cookies(driver):
            log("❌ Cookie Injection Failed")
            return

        # 3. Process filtered symbols
        for _, row in df_mv2.iterrows():
            symbol = safe_str(row.iloc[0])
            if safe_int(row.get("D_Trigger", 0)) != 1:
                continue

            log(f"🚀 Processing: {symbol}")

            # Screenshots for both timeframes
            tasks = [("day", day_urls.get(symbol)), ("week", week_urls.get(symbol))]
            
            for tf_name, url in tasks:
                if url and "tradingview.com" in url:
                    try:
                        driver.get(url)
                        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                        )
                        time.sleep(POST_LOAD_SLEEP)
                        save_screenshot(db, symbol, tf_name, chart.screenshot_as_png)
                    except Exception as e:
                        log(f"❌ Error {symbol} {tf_name}: {e}")

        log("🏁 All triggers processed.")
    finally:
        driver.quit()
        db.close()

if __name__ == "__main__":
    main()
