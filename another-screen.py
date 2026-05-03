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

# Path to keep you logged in (creates a folder in your script directory)
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
    
    # SESSION PERSISTENCE: This uses your saved login data
    options.add_argument(f"--user-data-dir={USER_DATA_DIR}")
    options.add_argument("--profile-directory=Default")
    
    # Hide automation flags
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
        INSERT INTO another_screenshot (symbol, timeframe, screenshot)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot)
        """
        cursor.execute(query, (symbol, timeframe, img))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# ---------------- CORE ---------------- #
def check_login_status(driver, symbol):
    """Checks if we are logged in by looking for the user profile/header."""
    try:
        # Looking for common 'logged in' indicators like user menu or sign out button
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='header-user-menu']"))
        )
        log("🔑 LOGIN VERIFIED: Session is active", symbol)
        return True
    except:
        log("⚠️ LOGIN WARNING: Running as Guest or Login not detected", symbol)
        return False

def take_screenshot(driver, symbol, tf, url):
    try:
        log(f"🌐 VISITING: {url}", symbol, tf)
        driver.get(url)

        # 1. Wait for chart base
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "canvas"))
        )
        
        # 2. Check Login (only on the first load of the symbol)
        check_login_status(driver, symbol)

        # 3. STABILITY WAIT: Let the 'exclamation' / loading indicators clear
        # TradingView usually needs time to fetch data once the canvas is drawn
        time.sleep(8) 

        # 4. Final Verification
        log("✅ Data stabilization complete", symbol, tf)
        
        img = driver.get_screenshot_as_png()
        save_to_mysql(symbol, tf, img)
        log("📸 SUCCESS: Screenshot saved to DB", symbol, tf)

    except Exception as e:
        log(f"❌ ERROR: {str(e)}", symbol, tf)
        # Optional: Save error screenshot for manual debugging
        driver.save_screenshot(f"error_{symbol}_{tf}.png")

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
        if day_url:
            take_screenshot(driver, symbol, "day", day_url)

        if week_url:
            take_screenshot(driver, symbol, "week", week_url)

    finally:
        driver.quit()
        log("⛔ FINISHED SYMBOL PROCESS", symbol)

# ---------------- LOAD SHEET ---------------- #
def load_rows():
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)
    ws = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    data = ws.get_all_values()

    headers = data[0]
    rows = pd.DataFrame(data[1:], columns=headers).to_dict("records")
    return rows

# ---------------- MAIN ---------------- #
def main():
    log("🚀 INITIALIZING SCRAPER ENGINE")
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
