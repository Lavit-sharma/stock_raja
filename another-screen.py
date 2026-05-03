import os
import time
import json
import gspread
import concurrent.futures
import re
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

MAX_THREADS = int(os.getenv("MAX_THREADS", "4"))
START_ROW = int(os.getenv("START_ROW", "0"))
END_ROW = int(os.getenv("END_ROW", "999999"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
TRUNCATE_ON_START = os.getenv("TRUNCATE_ON_START", "0") == "1"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "connect_timeout": 10,
    "autocommit": False
}

# ---------------- DB POOL ---------------- #
db_pool = None
try:
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name=f"screenshot_pool_{START_ROW}_{END_ROW}",
        pool_size=max(MAX_THREADS + 2, 5),
        **DB_CONFIG
    )
except Exception as e:
    print(f"[FATAL] Could not initialize DB Pool: {e}", flush=True)

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOGGING ---------------- #
RUN_ID = datetime.now().strftime("%Y%m%d-%H%M%S")

def log(msg, symbol="-", tf="-"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(
        f"[{ts}] [{RUN_ID}] [Rows {START_ROW}-{END_ROW}] [{symbol}] [{tf}] {msg}",
        flush=True
    )

def short_exc(e: Exception, max_len=220):
    s = f"{type(e).__name__}: {e}"
    return (s[:max_len] + "...") if len(s) > max_len else s

# ---------------- HELPERS ---------------- #

def make_unique_headers(headers):
    seen = {}
    out = []
    for h in headers:
        key = (h or "").strip()
        if key == "":
            key = "col"
        if key in seen:
            seen[key] += 1
            out.append(f"{key}_{seen[key]}")
        else:
            seen[key] = 1
            out.append(key)
    return out

def get_month_name(date_str):
    try:
        clean_date = re.sub(r'[*]', '', str(date_str)).strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(clean_date, fmt)
                return dt.strftime('%B')
            except ValueError:
                continue
        return "Unknown"
    except Exception:
        return "Unknown"

def get_db_connection():
    if not db_pool:
        return None
    return db_pool.get_connection()

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if not image_data or len(image_data) < 5000:
        log("❌ DB Save Aborted: Screenshot too small", symbol, timeframe)
        return False

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn: return False
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date, month_before)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                month_before = VALUES(month_before),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, chart_date, month_val))
        conn.commit()
        return True
    except Exception as err:
        if conn: conn.rollback()
        log(f"❌ DB Save Error: {short_exc(err)}", symbol, timeframe)
        return False
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(90)
    return driver

def inject_tv_cookies(driver, symbol="-"):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("⚠️ Missing TRADINGVIEW_COOKIES", symbol)
            return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        for c in cookies:
            try:
                driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
            except: continue
        driver.refresh()
        time.sleep(3)
        return True
    except Exception as e:
        log(f"⚠️ Cookie Error: {short_exc(e)}", symbol)
        return False

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
        log(f"🌐 Loading {timeframe}: {url}", symbol, timeframe)
        driver.get(url)
        wait = WebDriverWait(driver, 45)
        
        # Ensure chart container is loaded
        chart_xpath = "//div[contains(@class,'chart-container') or contains(@class,'chart-gui-wrapper')]"
        chart_element = wait.until(EC.presence_of_element_located((By.XPATH, chart_xpath)))
        
        # Trigger Go To Date
        ActionChains(driver).move_to_element(chart_element).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()
        
        # Fill Date
        input_xpath = "//input[contains(@class,'query') or @data-role='search' or contains(@class,'input')]"
        goto_input = wait.until(EC.element_to_be_clickable((By.XPATH, input_xpath)))
        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(str(target_date))
        goto_input.send_keys(Keys.ENTER)

        log("⏳ Waiting for render...", symbol, timeframe)
        time.sleep(12) 

        img = driver.get_screenshot_as_png()
        if save_to_mysql(symbol, timeframe, img, target_date, month_val):
            log("✅ Saved to DB", symbol, timeframe)
    except Exception as e:
        log(f"❌ Screenshot Error: {short_exc(e)}", symbol, timeframe)

def process_row(row, actual_index):
    symbol = str(row.get("Symbol", "")).strip()
    target_date = str(row.get("dates", "")).strip()
    day_url = str(row.get("Day", "")).strip()
    week_url = str(row.get("Week", "")).strip()

    if not symbol or not target_date: return

    driver = None
    try:
        driver = get_driver()
        if not inject_tv_cookies(driver, symbol): return
        month_name = get_month_name(target_date)

        if day_url and "tradingview.com" in day_url:
            navigate_and_snap(driver, symbol, "day", day_url, target_date, month_name)
            time.sleep(2)
        if week_url and "tradingview.com" in week_url:
            navigate_and_snap(driver, symbol, "week", week_url, target_date, month_name)

    except Exception as e:
        log(f"❌ Row Error at {actual_index}: {short_exc(e)}", symbol)
    finally:
        if driver:
            try: driver.quit()
            except: pass

def main():
    if not db_pool:
        log("❌ Connection pool failed.")
        return

    # Load Google Sheets Data
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
        data = worksheet.get_all_values()
        headers = make_unique_headers(data[0])
        all_rows = pd.DataFrame(data[1:], columns=headers).to_dict("records")
        log(f"✅ Loaded total rows: {len(all_rows)}")
    except Exception as e:
        log(f"❌ Load Error: {short_exc(e)}")
        return

    # Slice Rows
    selected_rows = all_rows[START_ROW:END_ROW]
    if not selected_rows:
        log("⚠️ No rows in range.")
        return

    # Process everything within ONE Executor to ensure script waits for all workers
    indexed_rows = list(enumerate(selected_rows, start=START_ROW + 1))
    
    log(f"🚀 Processing {len(indexed_rows)} rows with {MAX_THREADS} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Map all rows to the executor
        futures = [executor.submit(process_row, row, idx) for idx, row in indexed_rows]
        
        # This loop forces the main thread to wait and report results
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result() 
            except Exception as e:
                log(f"⚠️ Worker failure: {short_exc(e)}")

    log("🏁 Finished all tasks.")

if __name__ == "__main__":
    main()
