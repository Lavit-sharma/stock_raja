import os
import time
import json
import hashlib
import gspread
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from datetime import datetime
import pytz
import sys

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- TIME CONTROL (IST) ---------------- #
def is_allowed_time():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    print(f"[TIME] Current IST: {now.strftime('%Y-%m-%d %H:%M:%S')}")

    if 9 <= now.hour <= 16:
        print("✅ Within allowed time (9 AM – 4 PM IST)")
        return True
    else:
        print("⛔ Outside allowed time. Exiting...")
        return False

if not is_allowed_time():
    sys.exit()

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
    "connect_timeout": 20,
    "autocommit": True
}

CHART_WAIT_SEC = 25
POST_LOAD_SLEEP = 4

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def get_hash(symbol, change_val):
    return hashlib.sha256(f"{symbol}_day_{change_val}".encode()).hexdigest()

# ---------------- DB MANAGER ---------------- #
class DBManager:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        for i in range(3):
            try:
                log(f"🔗 Connecting DB (Attempt {i+1})")
                self.conn = mysql.connector.connect(**self.config)
                log("✅ DB Connected")
                return
            except Exception as e:
                log(f"⚠️ DB Error: {e}")
                time.sleep(5)
        raise Exception("❌ DB Connection Failed")

    def get_conn(self):
        if not self.conn or not self.conn.is_connected():
            self.connect()
        return self.conn

# ---------------- DRIVER ---------------- #
def get_driver():
    log("🌐 Launching Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

# ---------------- MAIN ---------------- #
def main():
    db = DBManager(DB_CONFIG)
    driver = None

    try:
        db.connect()

        # 📄 Google Sheet
        log("📄 Loading Google Sheet...")
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)

        data = ws.get_all_values()
        df = pd.DataFrame(data[1:])
        url_map = dict(zip(df[0].str.upper().str.strip(), df[3]))

        log(f"📊 Loaded {len(df)} rows")

        # 🔍 DB Query
        conn = db.get_conn()
        cur = conn.cursor(dictionary=True)

        cur.execute(f"""
            SELECT Symbol, real_close, real_change 
            FROM `{SOURCE_TABLE}` 
            WHERE CAST(real_change AS DECIMAL(10,2)) >= %s
        """, (CHANGE_THRESHOLD,))

        stocks = cur.fetchall()
        cur.close()

        if not stocks:
            log("😴 No stocks found")
            return

        log(f"🚀 {len(stocks)} stocks found")

        # 🧹 Clean table
        conn = db.get_conn()
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE `{TARGET_TABLE}`")
        cur.close()

        # 🌐 Browser
        driver = get_driver()
        driver.get("https://www.tradingview.com/")
        time.sleep(2)

        # 🍪 Cookies
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))
        for c in cookies:
            driver.add_cookie({
                "name": c["name"],
                "value": c["value"],
                "domain": ".tradingview.com",
                "path": "/"
            })
        driver.refresh()

        success = 0

        for stock in stocks:
            symbol = stock["Symbol"].upper().strip()
            change_val = stock["real_change"]
            url = url_map.get(symbol)

            if not url:
                continue

            try:
                log(f"📸 {symbol} ({change_val}%)")

                driver.get(url)

                WebDriverWait(driver, CHART_WAIT_SEC).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "chart-container"))
                )

                time.sleep(POST_LOAD_SLEEP)

                img = driver.get_screenshot_as_png()

                conn = db.get_conn()
                cur = conn.cursor()

                cur.execute(f"""
                    INSERT INTO `{TARGET_TABLE}`
                    (symbol, timeframe, real_change, real_close, screenshot)
                    VALUES (%s, %s, %s, %s, %s)
                """, (symbol, "day", change_val, stock["real_close"], img))

                cur.close()
                success += 1

            except Exception as e:
                log(f"⚠️ Error {symbol}: {e}")

        log(f"🏁 Done. Saved: {success}")

    except Exception as e:
        log(f"🚨 ERROR: {e}")

    finally:
        if driver:
            driver.quit()
        log("🛑 Finished")

if __name__ == "__main__":
    main()
