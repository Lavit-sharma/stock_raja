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
END_ROW = int(os.getenv("END_ROW", "0")) 
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
TRUNCATE_ON_START = os.getenv("TRUNCATE_ON_START", "0") == "1"

if os.getenv("GITHUB_ACTIONS") == "true":
    WP_UPLOAD_DIR = os.path.join(os.getcwd(), "screenshots")
    log_env = "GitHub Action"
else:
    WP_UPLOAD_DIR = "/var/www/html/wp-content/uploads/trading_charts"
    log_env = "Local/VPS Server"

WP_BASE_URL = os.getenv("WP_BASE_URL", "https://yourdomain.com/wp-content/uploads/trading_charts")
os.makedirs(WP_UPLOAD_DIR, exist_ok=True)

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
        pool_size=max(MAX_THREADS + 2, 10),
        **DB_CONFIG
    )
except Exception as e:
    print(f"[FATAL] Could not initialize DB Pool: {e}", flush=True)

CHROME_DRIVER_PATH = ChromeDriverManager().install()

def log(msg, symbol="-", tf="-"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{log_env}] [Rows {START_ROW}-{END_ROW}] [{symbol}] [{tf}] {msg}", flush=True)

def short_exc(e: Exception, max_len=220):
    s = f"{type(e).__name__}: {e}"
    return (s[:max_len] + "...") if len(s) > max_len else s

# ---------------- HELPERS ---------------- #

def save_image_to_disk(symbol, timeframe, image_bytes):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{symbol}_{timeframe}_{timestamp}.png".replace("/", "_")
        file_path = os.path.join(WP_UPLOAD_DIR, filename)
        with open(file_path, "wb") as f:
            f.write(image_bytes)
        return f"{WP_BASE_URL}/{filename}"
    except Exception as e:
        log(f"❌ Save Error: {short_exc(e)}", symbol, timeframe)
        return None

def make_unique_headers(headers):
    seen = {}
    out = []
    for h in headers:
        key = (h or "").strip()
        if key == "": key = "col"
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
            except: continue
        return "Unknown"
    except: return "Unknown"

def get_db_connection():
    return db_pool.get_connection() if db_pool else None

def save_to_mysql(symbol, timeframe, image_url, chart_date, month_val):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn: return False
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot_path, chart_date, month_before)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                screenshot_path = VALUES(screenshot_path),
                chart_date = VALUES(chart_date),
                month_before = VALUES(month_before),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_url, chart_date, month_val))
        conn.commit()
        return True
    except Exception as err:
        if conn: conn.rollback()
        log(f"❌ DB Error: {short_exc(err)}", symbol, timeframe)
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
            try:
                driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
            except: continue
        driver.refresh()
        return True
    except: return False

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
        log(f"🌐 Loading {timeframe}", symbol, timeframe)
        driver.get(url)
        time.sleep(8)
        
        wait = WebDriverWait(driver, 30)
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'chart-container')]")))
        ActionChains(driver).move_to_element(chart).click().perform()
        
        ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//input[contains(@class,'query')]")))
        goto_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        goto_input.send_keys(str(target_date) + Keys.ENTER)
        
        time.sleep(10)
        img_bytes = chart.screenshot_as_png
        url_path = save_image_to_disk(symbol, timeframe, img_bytes)
        if url_path:
            save_to_mysql(symbol, timeframe, url_path, target_date, month_val)
            log(f"✅ Saved", symbol, timeframe)
    except Exception as e:
        log(f"❌ Error: {short_exc(e)}", symbol, timeframe)

def process_row(row, actual_index):
    # CRITICAL: These keys must match your Google Sheet headers exactly!
    symbol = str(row.get("Symbol", "")).strip()
    target_date = str(row.get("dates", "")).strip()
    day_url = str(row.get("Day", "")).strip()
    week_url = str(row.get("Week", "")).strip()

    if not symbol or not target_date:
        log(f"⏩ Skipping Row {actual_index}: Missing Symbol or Date")
        return

    driver = None
    try:
        driver = get_driver()
        if inject_tv_cookies(driver):
            month_name = get_month_name(target_date)
            if day_url and "tradingview.com" in day_url:
                navigate_and_snap(driver, symbol, "day", day_url, target_date, month_name)
            if week_url and "tradingview.com" in week_url:
                navigate_and_snap(driver, symbol, "week", week_url, target_date, month_name)
    finally:
        if driver: driver.quit()

def main():
    if not db_pool: return
    
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
        data = worksheet.get_all_values()
        headers = make_unique_headers(data[0])
        all_rows = pd.DataFrame(data[1:], columns=headers).to_dict("records")
    except Exception as e:
        log(f"❌ GSheet Error: {short_exc(e)}")
        return

    selected_rows = all_rows[START_ROW:END_ROW]
    if not selected_rows:
        log("⚠️ No rows found in the selected range.")
        return

    log(f"🚀 Processing {len(selected_rows)} rows")
    indexed_rows = list(enumerate(selected_rows, start=START_ROW + 1))

    for i in range(0, len(indexed_rows), BATCH_SIZE):
        batch = indexed_rows[i : i + BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(process_row, r, idx) for idx, r in batch]
            concurrent.futures.wait(futures)

    log("🏁 Finished.")

if __name__ == "__main__":
    main()
