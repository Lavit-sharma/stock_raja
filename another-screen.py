# ✅ FINAL FIXED VERSION (WITH STRONG LOGGING + STABILITY)

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
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

MAX_THREADS = int(os.getenv("MAX_THREADS", "3"))
START_ROW = int(os.getenv("START_ROW", "0"))
END_ROW = int(os.getenv("END_ROW", "999999"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

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
    pool_size=5,
    **DB_CONFIG
)

CHROME_DRIVER_PATH = ChromeDriverManager().install()
RUN_ID = datetime.now().strftime("%Y%m%d-%H%M%S")

# ---------------- LOG ---------------- #
def log(msg, symbol="-", tf="-"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{symbol}] [{tf}] {msg}", flush=True)

# ---------------- DRIVER ---------------- #
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=options)

# ---------------- HELPERS ---------------- #
def get_db_connection():
    return db_pool.get_connection()


def save_to_mysql(symbol, timeframe, img):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
    INSERT INTO another_screenshot (symbol, timeframe, screenshot)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot)
    """

    cursor.execute(query, (symbol, timeframe, img))
    conn.commit()
    cursor.close()
    conn.close()

# ---------------- CORE ---------------- #
def wait_chart(driver, symbol, tf):
    try:
        chart = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "canvas"))
        )
        log("✅ Chart Loaded", symbol, tf)
        return chart
    except:
        log("❌ Chart NOT loaded", symbol, tf)
        return None


def take_screenshot(driver, symbol, tf, url):
    try:
        log(f"🌐 Opening URL", symbol, tf)
        driver.get(url)

        time.sleep(12)

        chart = wait_chart(driver, symbol, tf)
        if not chart:
            driver.save_screenshot(f"/tmp/{symbol}_{tf}_fail.png")
            return

        img = driver.get_screenshot_as_png()

        save_to_mysql(symbol, tf, img)
        log("📸 Screenshot saved", symbol, tf)

    except Exception as e:
        log(f"❌ Error: {str(e)}", symbol, tf)

# ---------------- PROCESS ROW ---------------- #
def process_row(row, idx):
    symbol = row.get("Symbol", "").strip()
    day_url = row.get("Day", "").strip()
    week_url = row.get("Week", "").strip()

    if not symbol:
        return

    log("▶️ START", symbol)

    driver = get_driver()

    try:
        if day_url:
            take_screenshot(driver, symbol, "day", day_url)

        if week_url:
            take_screenshot(driver, symbol, "week", week_url)

    finally:
        driver.quit()
        log("⛔ END", symbol)

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
    rows = load_rows()

    selected = rows[START_ROW:END_ROW]
    log(f"Total rows: {len(selected)}")

    for i in range(0, len(selected), BATCH_SIZE):
        batch = selected[i:i+BATCH_SIZE]
        log(f"🚀 Batch {i} to {i+len(batch)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
            futures = [ex.submit(process_row, row, idx) for idx, row in enumerate(batch)]

            for f in futures:
                f.result()

    log("🏁 DONE")


if __name__ == "__main__":
    main()
