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

# --- WORDPRESS FILE STORAGE CONFIG --- #
# Path on the server where Python saves the file
WP_UPLOAD_DIR = os.getenv("WP_UPLOAD_DIR", "/var/www/html/wp-content/uploads/trading_charts")
# Public URL prefix that points to the folder above
WP_BASE_URL = os.getenv("WP_BASE_URL", "https://yourdomain.com/wp-content/uploads/trading_charts")

# Automatically create folder if missing
if not os.path.exists(WP_UPLOAD_DIR):
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

def save_image_to_disk(symbol, timeframe, image_bytes):
    """Saves the screenshot as a PNG file and returns the public URL."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{symbol}_{timeframe}_{timestamp}.png".replace("/", "_")
        file_path = os.path.join(WP_UPLOAD_DIR, filename)
        
        with open(file_path, "wb") as f:
            f.write(image_bytes)
            
        return f"{WP_BASE_URL}/{filename}"
    except Exception as e:
        log(f"❌ Disk Save Error: {short_exc(e)}", symbol, timeframe)
        return None

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

def save_to_mysql(symbol, timeframe, image_url, chart_date, month_val):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            return False

        cursor = conn.cursor()
        # Updating to use the new screenshot_path column
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
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        log(f"❌ DB Save Error: {short_exc(err)}", symbol, timeframe)
        return False

    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--mute-audio")

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
        time.sleep(2)

        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c["name"],
                    "value": c["value"],
                    "domain": ".tradingview.com",
                    "path": "/"
                })
            except Exception:
                continue

        driver.refresh()
        time.sleep(2)
        return True

    except Exception as e:
        log(f"⚠️ Cookie Error: {short_exc(e)}", symbol)
        return False

def wait_for_chart_ready(driver):
    wait = WebDriverWait(driver, 35)
    chart = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//div[contains(@class,'chart-container') or contains(@class,'chart')]")
        )
    )
    return chart

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
        log(f"🌐 Loading {timeframe}: {url}", symbol, timeframe)
        driver.get(url)
        time.sleep(8)

        chart = wait_for_chart_ready(driver)
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)

        ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()

        wait = WebDriverWait(driver, 20)
        input_xpath = "//input[contains(@class,'query') or @data-role='search' or contains(@class,'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))

        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(str(target_date))
        goto_input.send_keys(Keys.ENTER)

        log("⏳ Waiting 10s for rendering...", symbol, timeframe)
        time.sleep(10)

        chart = wait_for_chart_ready(driver)
        img_bytes = chart.screenshot_as_png

        # Save binary to file
        image_url = save_image_to_disk(symbol, timeframe, img_bytes)

        if image_url:
            # Save path string to the new DB column
            if save_to_mysql(symbol, timeframe, image_url, target_date, month_val):
                log(f"✅ Path saved: {image_url}", symbol, timeframe)
            else:
                log("⚠️ File saved, but DB failed", symbol, timeframe)
        else:
            log("❌ File save failed", symbol, timeframe)

    except Exception as e:
        log(f"❌ Screenshot Error: {short_exc(e)}", symbol, timeframe)

def process_row(row, actual_index):
    symbol = str(row.get("Symbol", "")).strip()
    target_date = str(row.get("dates", "")).strip()
    day_url = str(row.get("Day", "")).strip()
    week_url = str(row.get("Week", "")).strip()

    if not symbol or not target_date:
        return

    driver = None
    try:
        driver = get_driver()
        if not inject_tv_cookies(driver, symbol):
            log("⚠️ Missing cookies", symbol)
            return

        month_name = get_month_name(target_date)

        if day_url and "tradingview.com" in day_url:
            navigate_and_snap(driver, symbol, "day", day_url, target_date, month_name)
            time.sleep(3)

        if week_url and "tradingview.com" in week_url:
            navigate_and_snap(driver, symbol, "week", week_url, target_date, month_name)

    except Exception as e:
        log(f"❌ Row Error: {short_exc(e)}", symbol)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

def truncate_table_if_needed():
    if not TRUNCATE_ON_START:
        return

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn: return
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE another_screenshot")
        conn.commit()
        log("✅ Table Truncated.")
    except Exception as e:
        log(f"⚠️ Truncate failed: {short_exc(e)}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def load_rows():
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)
    worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    data = worksheet.get_all_values()
    headers = make_unique_headers(data[0])
    rows = pd.DataFrame(data[1:], columns=headers).to_dict("records")
    return rows

def process_batch(batch_rows):
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_row, row, idx) for idx, row in batch_rows]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                log(f"⚠️ Worker failure: {short_exc(e)}")

def main():
    if not db_pool:
        log("❌ DB Pool failed.")
        return

    truncate_table_if_needed()

    try:
        all_rows = load_rows()
    except Exception as e:
        log(f"❌ GSheet Error: {short_exc(e)}")
        return

    selected_rows = all_rows[START_ROW:END_ROW]
    if not selected_rows:
        log("⚠️ No rows selected.")
        return

    log(f"✅ Processing {len(selected_rows)} rows in batches of {BATCH_SIZE}")
    indexed_rows = list(enumerate(selected_rows, start=START_ROW + 1))

    for batch_start in range(0, len(indexed_rows), BATCH_SIZE):
        batch = indexed_rows[batch_start:batch_start + BATCH_SIZE]
        process_batch(batch)

    log("🏁 All rows finished.")

if __name__ == "__main__":
    main()
