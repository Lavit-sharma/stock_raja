import os
import time
import json
import hashlib
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

SOURCE_TABLE = "wp_live_close"
TARGET_TABLE = "live_screen"
CHANGE_THRESHOLD = 7.0  # Trigger for >= 7%

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 5
DB_RETRY = 3

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def get_hash(symbol, timeframe, change_val):
    """Prevents duplicate screenshots if value hasn't changed."""
    data = f"{symbol}_{timeframe}_{change_val}"
    return hashlib.sha256(data.encode()).hexdigest()

class DB:
    def __init__(self, config):
        self.config = config
        self.conn = mysql.connector.connect(**self.config)
        self.conn.autocommit = True

    def ensure(self):
        if not self.conn.is_connected():
            self.conn.ping(reconnect=True)
        return self.conn

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    cookie_data = os.getenv("TRADINGVIEW_COOKIES")
    if not cookie_data: return False
    driver.get("https://www.tradingview.com/")
    time.sleep(2)
    for c in json.loads(cookie_data):
        driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
    driver.refresh()
    return True

# ---------------- PROCESSING ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        # 1. Get Stock URLs from GSheets
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        client = gspread.service_account_from_dict(creds)
        sheet = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df_stocks = pd.DataFrame(sheet.get_all_values()[1:])
        url_map = dict(zip(df_stocks[0].str.strip().str.upper(), zip(df_stocks[3], df_stocks[2])))

        # 2. Fetch stocks from WP table where real_change >= 7
        conn = db.ensure()
        cur = conn.cursor(dictionary=True)
        # Using CAST because real_change is longtext in your schema
        query = f"SELECT Symbol, real_close, real_change FROM `{SOURCE_TABLE}` WHERE CAST(real_change AS DECIMAL(10,2)) >= %s"
        cur.execute(query, (CHANGE_THRESHOLD,))
        triggered_stocks = cur.fetchall()
        cur.close()

        if not triggered_stocks:
            log("No stocks found with >= 7% change.")
            return

        # 3. Start Browser
        driver = get_driver()
        inject_tv_cookies(driver)

        for stock in triggered_stocks:
            symbol = stock['Symbol'].strip().upper()
            change_val = stock['real_change']
            
            if symbol not in url_map:
                continue

            # Process both Day and Week
            for i, tf in enumerate(["day", "week"]):
                url = url_map[symbol][i]
                new_hash = get_hash(symbol, tf, change_val)

                # Check if we already have this exact screenshot
                cur = conn.cursor()
                cur.execute(f"SELECT id FROM `{TARGET_TABLE}` WHERE change_hash = %s", (new_hash,))
                if cur.fetchone():
                    log(f"Skipping {symbol} {tf} - Already captured.")
                    cur.close()
                    continue
                cur.close()

                # Capture Screenshot
                log(f"Capturing {symbol} ({tf}) at {change_val}%")
                driver.get(url)
                try:
                    chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                    )
                    time.sleep(POST_LOAD_SLEEP)
                    img = chart.screenshot_as_png

                    # Save to live_screen
                    cur = conn.cursor()
                    insert_query = f"""INSERT INTO `{TARGET_TABLE}` 
                                    (symbol, timeframe, real_change, real_close, change_hash, screenshot) 
                                    VALUES (%s, %s, %s, %s, %s, %s)"""
                    cur.execute(insert_query, (symbol, tf, change_val, stock['real_close'], new_hash, img))
                    cur.close()
                except Exception as e:
                    log(f"Failed {symbol}: {e}")

    finally:
        if driver: driver.quit()
        log("Done.")

if __name__ == "__main__":
    main()
