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

# ---------------- CONFIG & TIME ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843
SOURCE_TABLE = "wp_live_close"
TARGET_TABLE = "live_screen"
CHANGE_THRESHOLD = 7.0 

def get_now_ist():
    return datetime.now(pytz.timezone("Asia/Kolkata"))

# Exit if not during market/processing hours
now = get_now_ist()
if not (9 <= now.hour <= 16):
    print(f"⛔ Outside allowed time ({now.strftime('%H:%M')}). Exiting...")
    sys.exit()

# ---------------- OPTIMIZED DRIVER ---------------- #
def get_optimized_driver():
    print("🌐 Launching Optimized Browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # OPTIMIZATION: Disable images (optional) or just ads to speed up load
    opts.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    return driver

# ---------------- MAIN ---------------- #
def main():
    conn = None
    driver = None
    
    try:
        # 1. Database Connection
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            autocommit=True
        )
        cur = conn.cursor(dictionary=True)

        # 2. Fetch Signals (Filtered by Threshold)
        cur.execute(f"SELECT Symbol, real_close, real_change FROM `{SOURCE_TABLE}` WHERE CAST(real_change AS DECIMAL(10,2)) >= %s", (CHANGE_THRESHOLD,))
        stocks = cur.fetchall()
        
        if not stocks:
            print("😴 No high-growth stocks found. Sleeping.")
            return

        # 3. Load URL Map from Google Sheets
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        ws = gc.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df = pd.DataFrame(ws.get_all_values()[1:])
        url_map = dict(zip(df[0].str.upper().str.strip(), df[3]))

        # 4. Check what we ALREADY processed today (To avoid re-doing work if stuck)
        today_str = get_now_ist().strftime('%Y-%m-%d')
        cur.execute(f"SELECT symbol FROM `{TARGET_TABLE}` WHERE DATE(created_at) = %s", (today_str,))
        processed_today = {row['symbol'] for row in cur.fetchall()}

        # 5. Setup Browser with TradingView Session
        driver = get_optimized_driver()
        driver.get("https://www.tradingview.com/")
        
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()

        success_count = 0
        for stock in stocks:
            symbol = stock["Symbol"].upper().strip()
            if symbol in processed_today:
                print(f"⏩ Skipping {symbol} (Already captured today)")
                continue
                
            url = url_map.get(symbol)
            if not url: continue

            try:
                print(f"📸 Capturing {symbol}...", end=" ", flush=True)
                driver.get(url)

                # OPTIMIZATION: Wait for the specific chart layout, not just the container
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container-border')]")))
                
                # Shorter sleep, just enough for candles to render
                time.sleep(3) 

                img_data = driver.get_screenshot_as_png()
                ist_now = get_now_ist().strftime('%Y-%m-%d %H:%M:%S')

                # Use the same cursor to insert
                cur.execute(f"""
                    INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, real_change, real_close, screenshot, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE screenshot=%s, created_at=%s
                """, (symbol, "day", stock["real_change"], stock["real_close"], img_data, ist_now, img_data, ist_now))
                
                print("✅")
                success_count += 1

            except Exception as e:
                print(f"❌ Failed: {str(e)[:50]}")

        print(f"🏁 Finished. New captures: {success_count}")

    except Exception as e:
        print(f"🚨 CRITICAL ERROR: {e}")
    finally:
        if conn: conn.close()
        if driver: driver.quit()

if __name__ == "__main__":
    main()
