import os
import time
import json
import base64
import requests
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
from PIL import Image
from io import BytesIO

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

MAX_THREADS = int(os.getenv("MAX_THREADS", "4"))
START_ROW = int(os.getenv("START_ROW", "0"))
END_ROW = int(os.getenv("END_ROW", "0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

WP_BASE_URL = os.getenv("WP_BASE_URL")  # https://rajakrishna.in

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
db_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="screenshot_pool",
    pool_size=max(MAX_THREADS + 2, 10),
    **DB_CONFIG
)

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOG ---------------- #
def log(msg, symbol="-", tf="-"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{symbol}] [{tf}] {msg}", flush=True)

# ---------------- AUTH ---------------- #
def get_auth_header():
    username = os.getenv("WP_USER")
    password = os.getenv("WP_APP_PASSWORD")
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {token}"

# ---------------- IMAGE ---------------- #
def compress_image(image_bytes):
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=60)
    return buffer.getvalue()

# ---------------- WORDPRESS UPLOAD ---------------- #
def upload_to_wordpress(image_bytes, filename, symbol):
    url = f"{WP_BASE_URL}/wp-json/wp/v2/media"

    # pseudo-folder using slug
    slug = f"anotherscreenshots-{symbol.lower()}"

    headers = {
        "Authorization": get_auth_header(),
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": "image/jpeg"
    }

    params = {
        "slug": slug
    }

    try:
        response = requests.post(url, headers=headers, params=params, data=image_bytes, timeout=30)

        if response.status_code in [200, 201]:
            return response.json().get("source_url")
        else:
            print("❌ WP Upload Error:", response.status_code, response.text)
            return None

    except Exception as e:
        print("❌ Upload Exception:", str(e))
        return None

# ---------------- DB SAVE ---------------- #
def get_db_connection():
    return db_pool.get_connection()

def save_to_mysql(symbol, timeframe, image_url, chart_date, month_val):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            INSERT INTO another_screenshot (symbol, timeframe, img_path, chart_date, month_before)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                img_path = VALUES(img_path),
                chart_date = VALUES(chart_date),
                month_before = VALUES(month_before),
                created_at = CURRENT_TIMESTAMP
        """

        cursor.execute(query, (symbol, timeframe, image_url, chart_date, month_val))
        conn.commit()
        return True

    except Exception as err:
        if conn: conn.rollback()
        log(f"❌ DB Error: {err}", symbol, timeframe)
        return False

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

# ---------------- HELPERS ---------------- #
def get_month_name(date_str):
    try:
        dt = datetime.strptime(str(date_str), "%Y-%m-%d")
        return dt.strftime('%B')
    except:
        return "Unknown"

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
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

        # 📸 Screenshot
        img_bytes = chart.screenshot_as_png

        # 🔥 Compress
        img_bytes = compress_image(img_bytes)

        filename = f"{symbol}_{timeframe}.jpg".replace("/", "_")

        # 🚀 Upload
        image_url = upload_to_wordpress(img_bytes, filename, symbol)

        if image_url:
            save_to_mysql(symbol, timeframe, image_url, target_date, month_val)
            log("✅ Uploaded", symbol, timeframe)
        else:
            log("❌ Upload failed", symbol, timeframe)

    except Exception as e:
        log(f"❌ Error: {e}", symbol, timeframe)

# ---------------- PROCESS ---------------- #
def process_row(row):
    symbol = str(row.get("Symbol", "")).strip()
    target_date = str(row.get("dates", "")).strip()
    day_url = str(row.get("Day", "")).strip()

    if not symbol or not target_date:
        return

    driver = None
    try:
        driver = get_driver()
        month_name = get_month_name(target_date)

        if day_url:
            navigate_and_snap(driver, symbol, "day", day_url, target_date, month_name)

    finally:
        if driver:
            driver.quit()

# ---------------- MAIN ---------------- #
def main():
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)
    worksheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)

    data = worksheet.get_all_records()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(process_row, data)

    print("🏁 Done")

if __name__ == "__main__":
    main()
