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
    "connect_timeout": 30,
    "autocommit": True
}

CHART_WAIT_SEC = 25
POST_LOAD_SLEEP = 4

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def get_hash(symbol, change_val):
    # Only Day timeframe now, so hash is simpler
    data = f"{symbol}_day_{change_val}"
    return hashlib.sha256(data.encode()).hexdigest()

class DBManager:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if self.conn: self.conn.close()
            self.conn = mysql.connector.connect(**self.config)
            log("✅ Database Connected.")
        except mysql.connector.Error as err:
            log(f"❌ DB Connection Error: {err}")
            raise

    def get_conn(self):
        try:
            if not self.conn or not self.conn.is_connected():
                self.connect()
            else:
                self.conn.ping(reconnect=True, attempts=3, delay=2)
        except:
            self.connect()
        return self.conn

def get_driver():
    log("🌐 Initializing Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def main():
    db = DBManager(DB_CONFIG)
    driver = None

    try:
        # 1. Load URLs from GSheet (Column 3 is Day URL)
        log("📄 Loading URLs from Google Sheets...")
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df_sheet = pd.DataFrame(ws.get_all_values()[1:])
        url_map = dict(zip(df_sheet[0].str.strip().str.upper(), df_sheet[3]))

        # 2. Identify Stocks >= 7%
        conn = db.get_conn()
        cur = conn.cursor(dictionary=True)
        query = f"SELECT Symbol, real_close, real_change FROM `{SOURCE_TABLE}` WHERE CAST(real_change AS DECIMAL(10,2)) >= %s"
        cur.execute(query, (CHANGE_THRESHOLD,))
        triggered_stocks = cur.fetchall()
        cur.close()

        if not triggered_stocks:
            log("😴 No stocks match criteria.")
            return

        log(f"🚀 Found {len(triggered_stocks)} stocks. Processing Day Charts...")

        # 3. Setup Browser & Cookies
        driver = get_driver()
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        for c in json.loads(os.getenv("TRADINGVIEW_COOKIES")):
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        log("✅ TradingView Session Ready.")

        # 4. Process Loop
        success_count = 0
        for stock in triggered_stocks:
            symbol = stock['Symbol'].strip().upper()
            change_val = stock['real_change']
            day_url = url_map.get(symbol)

            if not day_url: continue

            # Deduplication Check
            new_hash = get_hash(symbol, change_val)
            conn = db.get_conn()
            cur = conn.cursor()
            cur.execute(f"SELECT id FROM `{TARGET_TABLE}` WHERE change_hash = %s", (new_hash,))
            if cur.fetchone():
                cur.close()
                continue
            cur.close()

            # Capture & Save
            try:
                log(f"📸 Capturing: {symbol} ({change_val}%)")
                driver.get(day_url)
                WebDriverWait(driver, CHART_WAIT_SEC).until(
                    EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                )
                time.sleep(POST_LOAD_SLEEP)
                img = driver.get_screenshot_as_png()

                # Optimized DB Insert
                conn = db.get_conn()
                cur = conn.cursor()
                sql = f"INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, real_change, real_close, change_hash, screenshot) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(sql, (symbol, 'day', change_val, stock['real_close'], new_hash, img))
                cur.close()
                success_count += 1
            except Exception as e:
                log(f"⚠️ Failed {symbol}: {e}")

        log(f"✅ Successfully processed {success_count} new charts.")

    except Exception as e:
        log(f"🚨 FATAL ERROR: {e}")
    finally:
        if driver: driver.quit()
        log("🏁 Process Finished.")

if __name__ == "__main__":
    main()
