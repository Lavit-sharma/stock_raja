import os
import time
import json
import gspread
import concurrent.futures
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

MAX_THREADS = int(os.getenv("MAX_THREADS", "3"))
START_ROW = int(os.getenv("START_ROW", "0"))
END_ROW = int(os.getenv("END_ROW", "999999"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
TRADINGVIEW_COOKIES_JSON = os.getenv("TRADINGVIEW_COOKIES")

USER_DATA_DIR = os.path.join(os.getcwd(), "chrome_profile")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

# ---------------- DB POOL ---------------- #
db_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="screenshot_pool",
    pool_size=10,
    **DB_CONFIG
)

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOGGING ---------------- #
def log(msg, symbol="-", tf="-"):
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] [{symbol}] [{tf}] {msg}", flush=True)

# ---------------- DRIVER ---------------- #
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    options.add_argument("--profile-directory=Default")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=options)

# ---------------- HELPERS ---------------- #
def get_db_connection():
    return db_pool.get_connection()

def save_to_mysql(symbol, timeframe, img):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = """
        INSERT INTO another_screenshot (symbol, timeframe, screenshot, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE 
            screenshot=VALUES(screenshot),
            updated_at=NOW()
        """
        cursor.execute(query, (symbol, timeframe, img))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def load_cookies(driver):
    """Parses TRADINGVIEW_COOKIES env and injects them into the driver."""
    if not TRADINGVIEW_COOKIES_JSON:
        log("⚠️ No TRADINGVIEW_COOKIES found in environment", "COOKIES")
        return False
    
    try:
        # Selenium needs to be on the domain to set cookies
        driver.get("https://www.tradingview.com") 
        time.sleep(2)
        
        cookies = json.loads(TRADINGVIEW_COOKIES_JSON)
        for cookie in cookies:
            # Remove expiry for session-based stability if necessary
            if 'expiry' in cookie:
                del cookie['expiry']
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                log(f"Skipping cookie: {str(e)}", "COOKIES")
        
        log("✅ Cookies injected successfully", "COOKIES")
        driver.refresh()
        return True
    except Exception as e:
        log(f"❌ Error loading cookies: {str(e)}", "COOKIES")
        return False

def remove_popups(driver):
    selectors = [
        "div[class*='overlap-manager']", 
        "div[class*='dialog-']", 
        "button[name='close']",
        ".tv-dialog__close",
        "[data-role='toast-container']",
        "div[id*='cookies-policy']"
    ]
    js_script = f"""
    var selectors = {json.dumps(selectors)};
    selectors.forEach(function(s) {{
        var elements = document.querySelectorAll(s);
        elements.forEach(function(el) {{ el.style.display = 'none'; }});
    }});
    """
    driver.execute_script(js_script)

# ---------------- CORE ---------------- #
def check_login_status(driver, symbol):
    try:
        # Check for user menu or a specific logged-in element
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='header-user-menu']"))
        )
        log("🔑 LOGIN VERIFIED", symbol)
        return True
    except:
        log("⚠️ LOGIN WARNING: Running as Guest", symbol)
        return False

def take_screenshot(driver, symbol, tf, url):
    try:
        log(f"🌐 VISITING: {url}", symbol, tf)
        driver.get(url)

        # 1. Wait for chart
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "canvas"))
        )
        
        check_login_status(driver, symbol)

        # 2. Stability wait for indicators
        time.sleep(8) 

        # 3. Clean UI before snap
        remove_popups(driver)
        log("🧹 Popups cleared", symbol, tf)

        # 4. Save
        img = driver.get_screenshot_as_png()
        save_to_mysql(symbol, tf, img)
        log("📸 SUCCESS: Entry Updated in DB", symbol, tf)

    except Exception as e:
        log(f"❌ ERROR: {str(e)}", symbol, tf)

# ---------------- PROCESS ROW ---------------- #
def process_row(row, idx):
    symbol = row.get("Symbol", "").strip()
    day_url = row.get("Day", "").strip()
    week_url = row.get("Week", "").strip()

    if not symbol:
        return

    log("▶️ STARTING SYMBOL PROCESS", symbol)
    driver = get_driver()

    try:
        # LOAD COOKIES ONCE PER DRIVER LIFECYCLE
        load_cookies(driver)

        if day_url:
            take_screenshot(driver, symbol, "day", day_url)

        if week_url:
            take_screenshot(driver, symbol, "week", week_url)

    finally:
        driver.quit()
        log("⛔ FINISHED SYMBOL PROCESS", symbol)

# ---------------- LOAD SHEET ---------------- #
def load_rows():
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if not creds_json:
        raise ValueError("GSPREAD_CREDENTIALS environment variable is not set.")
    
    creds = json.loads(creds_json)
    gc = gspread.service_account_from_dict(creds)
    ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    data = ws.get_all_values()

    headers = data[0]
    rows = pd.DataFrame(data[1:], columns=headers).to_dict("records")
    return rows

# ---------------- MAIN ---------------- #
def main():
    log("🚀 INITIALIZING SCRAPER ENGINE")
    
    # Handle Truncate Request if passed
    if os.getenv("TRUNCATE_ON_START") == "1":
        log("🧹 TRUNCATING TABLE...")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE another_screenshot")
        conn.commit()
        cursor.close()
        conn.close()
        if os.getenv("END_ROW") == "0": # Exit if this was just a truncate job
            return

    rows = load_rows()
    selected = rows[START_ROW:END_ROW]
    log(f"📋 TARGET DATA: {len(selected)} rows found")

    for i in range(0, len(selected), BATCH_SIZE):
        batch = selected[i:i+BATCH_SIZE]
        log(f"📦 PROCESSING BATCH: {i} to {i+len(batch)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
            futures = [ex.submit(process_row, row, idx) for idx, row in enumerate(batch)]
            for f in futures:
                f.result()

    log("🏁 ALL TASKS COMPLETED")

if __name__ == "__main__":
    main()
