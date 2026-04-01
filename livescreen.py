import os
import time
import json
import hashlib
import gspread
import pandas as pd
import mysql.connector
from mysql.connector import errorcode

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

SOURCE_TABLE = "wp_live_close"
TARGET_TABLE = "live_screen"
CHANGE_THRESHOLD = 7.0 

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 15  # Prevents GitHub Actions from hanging forever
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6

# ---------------- HELPERS ---------------- #
def log(msg):
    timestamp = time.strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

def get_hash(symbol, timeframe, change_val):
    data = f"{symbol}_{timeframe}_{change_val}"
    return hashlib.sha256(data.encode()).hexdigest()

class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            log(f"🔗 Attempting connection to {self.config['host']}...")
            self.conn = mysql.connector.connect(**self.config)
            self.conn.autocommit = True
            log("✅ Database Connected.")
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                log("❌ DB Error: Bad username or password.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                log("❌ DB Error: Database does not exist.")
            elif err.errno == 2003:
                log("❌ DB Error: Connection Refused. Check 'Remote MySQL' settings and whitelist '%' in your hosting panel.")
            else:
                log(f"❌ DB Error: {err}")
            raise

    def ensure(self):
        if not self.conn or not self.conn.is_connected():
            log("🔄 Connection lost. Reconnecting...")
            self.connect()
        return self.conn

# ---------------- SELENIUM ---------------- #
def get_driver():
    log("🌐 Starting Chrome (Headless)...")
    CHROME_DRIVER_PATH = ChromeDriverManager().install()
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    log("🍪 Injecting TradingView Cookies...")
    cookie_data = os.getenv("TRADINGVIEW_COOKIES")
    if not cookie_data:
        log("⚠️ TRADINGVIEW_COOKIES secret is missing!")
        return False
    
    driver.get("https://www.tradingview.com/")
    time.sleep(2)
    try:
        cookies = json.loads(cookie_data)
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        log("✅ Cookies applied successfully.")
        return True
    except Exception as e:
        log(f"❌ Failed to apply cookies: {e}")
        return False

# ---------------- MAIN ---------------- #
def main():
    db = None
    driver = None

    try:
        # 1. Connect to DB
        db = DB(DB_CONFIG)
        conn = db.ensure()

        # 2. Get Stocks from GSheets
        log("📄 Fetching URL map from Google Sheets...")
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json: raise Exception("GSPREAD_CREDENTIALS missing!")
        
        client = gspread.service_account_from_dict(json.loads(creds_json))
        sheet = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df_stocks = pd.DataFrame(sheet.get_all_values()[1:])
        url_map = dict(zip(df_stocks[0].str.strip().str.upper(), zip(df_stocks[3], df_stocks[2])))

        # 3. Find stocks moving >= 7%
        log(f"🔍 Checking `{SOURCE_TABLE}` for movements >= {CHANGE_THRESHOLD}%...")
        cur = conn.cursor(dictionary=True)
        query = f"SELECT Symbol, real_close, real_change FROM `{SOURCE_TABLE}` WHERE CAST(real_change AS DECIMAL(10,2)) >= %s"
        cur.execute(query, (CHANGE_THRESHOLD,))
        triggered = cur.fetchall()
        cur.close()

        if not triggered:
            log("😴 No high-movement stocks found. Job done.")
            return

        log(f"🚀 Found {len(triggered)} stocks. Starting screenshots...")
        driver = get_driver()
        if not inject_tv_cookies(driver): return

        for stock in triggered:
            symbol = stock['Symbol'].strip().upper()
            change_val = stock['real_change']
            
            if symbol not in url_map:
                log(f"⚠️ URL not found for {symbol} in GSheet. Skipping.")
                continue

            for i, tf in enumerate(["day", "week"]):
                url = url_map[symbol][i]
                new_hash = get_hash(symbol, tf, change_val)

                # Deduplication Check
                cur = conn.cursor()
                cur.execute(f"SELECT id FROM `{TARGET_TABLE}` WHERE change_hash = %s", (new_hash,))
                if cur.fetchone():
                    log(f"⏭️  Already have {symbol} {tf} at {change_val}%. Skipping.")
                    cur.close()
                    continue
                cur.close()

                # Capture
                log(f"📸 Capturing {symbol} ({tf}) | {url}")
                try:
                    driver.get(url)
                    WebDriverWait(driver, CHART_WAIT_SEC).until(
                        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                    )
                    time.sleep(POST_LOAD_SLEEP)
                    img = driver.get_screenshot_as_png()

                    # Save to DB
                    cur = conn.cursor()
                    sql = f"INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, real_change, real_close, change_hash, screenshot) VALUES (%s, %s, %s, %s, %s, %s)"
                    cur.execute(sql, (symbol, tf, change_val, stock['real_close'], new_hash, img))
                    cur.close()
                    log(f"✅ Saved {symbol} {tf}")
                except Exception as e:
                    log(f"❌ Error capturing {symbol}: {e}")

    except Exception as e:
        log(f"🚨 FATAL ERROR: {e}")
    finally:
        if driver: driver.quit()
        log("🏁 Process Finished.")

if __name__ == "__main__":
    main()
