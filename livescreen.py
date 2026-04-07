import os
import time
import json
import gspread
import pandas as pd
import mysql.connector
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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843
SOURCE_TABLE = "wp_live_close"
TARGET_TABLE = "live_screen"
CHANGE_THRESHOLD = 7.0 

def get_now_ist():
    return datetime.now(pytz.timezone("Asia/Kolkata"))

# ---------------- OPTIMIZED DRIVER ---------------- #
def get_optimized_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Speed up loading by ignoring unnecessary syncs
    opts.add_argument("--proxy-server='direct://'")
    opts.add_argument("--proxy-bypass-list=*")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver

# ---------------- MAIN ---------------- #
def main():
    driver = None
    db_conn = None
    
    try:
        # 1. Database Connection (Established once)
        print("🔗 Connecting to Database...")
        db_conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            autocommit=True,
            connection_timeout=30 # Increased timeout
        )
        cur = db_conn.cursor(dictionary=True)

        # 2. Fetch Signals
        cur.execute(f"SELECT Symbol, real_close, real_change FROM `{SOURCE_TABLE}` WHERE CAST(real_change AS DECIMAL(10,2)) >= %s", (CHANGE_THRESHOLD,))
        stocks = cur.fetchall()
        
        if not stocks:
            print("😴 No signals found.")
            return

        # 3. Load URL Map
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df = pd.DataFrame(ws.get_all_values()[1:])
        url_map = dict(zip(df[0].str.upper().str.strip(), df[3]))

        # 4. Setup Browser
        print(f"🚀 Processing {len(stocks)} stocks...")
        driver = get_optimized_driver()
        driver.get("https://www.tradingview.com/")
        
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()

        success_count = 0
        for stock in stocks:
            symbol = stock["Symbol"].upper().strip()
            url = url_map.get(symbol)
            if not url: continue

            try:
                # 🛡️ CHECK DB CONNECTION BEFORE EACH UPLOAD
                try:
                    db_conn.ping(reconnect=True, attempts=3, delay=2)
                except:
                    print("🔄 Reconnecting DB...")
                    db_conn = mysql.connector.connect(host=os.getenv("DB_HOST"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), database=os.getenv("DB_NAME"), autocommit=True)
                    cur = db_conn.cursor(dictionary=True)

                print(f"📸 {symbol}...", end=" ", flush=True)
                driver.get(url)

                # Wait for chart specifically
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "chart-container")))
                time.sleep(3) # Small buffer for candles

                img_data = driver.get_screenshot_as_png()
                ist_now = get_now_ist().strftime('%Y-%m-%d %H:%M:%S')

                # UPSERT: Update if exists, insert if not
                sql = f"""
                    INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, real_change, real_close, screenshot, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE screenshot=%s, created_at=%s, real_change=%s, real_close=%s
                """
                cur.execute(sql, (symbol, "day", stock["real_change"], stock["real_close"], img_data, ist_now, img_data, ist_now, stock["real_change"], stock["real_close"]))
                
                print("✅")
                success_count += 1

            except Exception as e:
                print(f"❌ Error: {str(e)[:40]}")

        print(f"🏁 Done. Total: {success_count}")

    except Exception as e:
        print(f"🚨 CRITICAL: {e}")
    finally:
        if driver: driver.quit()
        if db_conn: db_conn.close()

if __name__ == "__main__":
    main()
