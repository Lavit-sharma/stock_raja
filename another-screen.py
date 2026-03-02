import os, time, json, gspread, concurrent.futures, re, traceback, sys
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

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "connect_timeout": 10 
}

# --- DB POOL INITIALIZATION ---
db_pool = None
try:
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="screenshot_pool",
        pool_size=MAX_THREADS + 2,
        **DB_CONFIG
    )
except Exception as e:
    print(f"[FATAL] Could not initialize DB Pool: {e}")

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOGGING ---------------- #
RUN_ID = datetime.now().strftime("%Y%m%d-%H%M%S")

def log(msg, symbol="-", tf="-"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{RUN_ID}] [{symbol}] [{tf}] {msg}", flush=True)

def short_exc(e: Exception, max_len=160):
    s = f"{type(e).__name__}: {e}"
    return (s[:max_len] + "...") if len(s) > max_len else s

# ---------------- HELPERS ---------------- #

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
            except ValueError: continue
        return "Unknown"
    except: return "Unknown"

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if not db_pool: return False
    conn = None
    try:
        conn = db_pool.get_connection()
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
        cursor.close()
        return True
    except Exception as err:
        log(f"❌ DB Save Error: {short_exc(err)}", symbol, timeframe)
        return False
    finally:
        if conn: conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver, symbol="-"):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        for c in cookies:
            try:
                driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
            except: continue
        driver.refresh()
        return True
    except Exception as e:
        log(f"⚠️ Cookie Error: {short_exc(e)}", symbol)
        return False

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
        log(f"🌐 Loading {timeframe}: {url}", symbol, timeframe)
        driver.get(url)
        
        # Wait for the main page shell to load
        time.sleep(8) 

        wait = WebDriverWait(driver, 30)
        # Ensure chart is clickable
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'chart-container') or contains(@class,'chart')]")))
        
        # Click to focus
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        
        # Open "Go To" Dialog
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        # Locate input and enter date
        input_xpath = "//input[contains(@class,'query') or @data-role='search' or contains(@class,'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))
        goto_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        goto_input.send_keys(str(target_date) + Keys.ENTER)

        # CRITICAL: Wait for candles and indicators to draw after jumping to date
        log(f"⏳ Waiting 10s for candles/indicators to render...", symbol, timeframe)
        time.sleep(10)

        img = chart.screenshot_as_png
        if save_to_mysql(symbol, timeframe, img, target_date, month_val):
            log(f"✅ Saved {timeframe} to DB", symbol, timeframe)
        else:
            log(f"⚠️ {timeframe} captured but not saved", symbol, timeframe)

    except Exception as e:
        log(f"❌ {timeframe} Error: {short_exc(e)}", symbol, timeframe)

def process_row(row, idx):
    symbol = str(row.get("Symbol", "")).strip()
    target_date = str(row.get("dates", "")).strip()
    day_url = str(row.get("Day", "")).strip()
    week_url = str(row.get("Week", "")).strip() 
    
    if not symbol or not target_date: return

    driver = get_driver()
    try:
        if inject_tv_cookies(driver, symbol):
            month_name = get_month_name(target_date)
            
            # 1. Daily Chart
            if day_url and "tradingview.com" in day_url:
                navigate_and_snap(driver, symbol, "day", day_url, target_date, month_name)
                time.sleep(3) # Short buffer between navigations
            
            # 2. Weekly Chart
            if week_url and "tradingview.com" in week_url:
                navigate_and_snap(driver, symbol, "week", week_url, target_date, month_name)
                
    finally:
        driver.quit()

# ---------------- MAIN ---------------- #

def main():
    if not db_pool:
        log("❌ Connection Pool failed. Check DB config.", "-", "-")

    try:
        if db_pool:
            conn = db_pool.get_connection()
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE another_screenshot")
            conn.commit()
            cursor.close()
            conn.close()
            log("✅ Table Truncated.")
    except Exception as e:
        log(f"⚠️ Truncate failed: {short_exc(e)}")

    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
        data = worksheet.get_all_values()
        headers = make_unique_headers(data[0])
        rows = pd.DataFrame(data[1:], columns=headers).to_dict("records")
        log(f"✅ Loaded {len(rows)} rows.")
    except Exception as e:
        log(f"❌ Google Sheet Error: {short_exc(e)}")
        return

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(lambda r: process_row(r[1], r[0]), enumerate(rows, 1))

    log("🏁 Finished.")

if __name__ == "__main__":
    main()
