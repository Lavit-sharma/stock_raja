import os
import time
import json
import base64
import requests
import gspread
import concurrent.futures
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
from PIL import Image
from io import BytesIO

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

MAX_THREADS = int(os.getenv("MAX_THREADS", "3"))

WP_BASE_URL = os.getenv("WP_BASE_URL")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

# ---------------- DB ---------------- #
db_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="pool",
    pool_size=5,
    **DB_CONFIG
)

def get_db():
    return db_pool.get_connection()

# ---------------- LOG ---------------- #
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- AUTH ---------------- #
def get_auth():
    token = base64.b64encode(
        f"{os.getenv('WP_USER')}:{os.getenv('WP_APP_PASSWORD')}".encode()
    ).decode()
    return {"Authorization": f"Basic {token}"}

# ---------------- IMAGE ---------------- #
def compress_image(img_bytes):
    img = Image.open(BytesIO(img_bytes)).convert("RGB")
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=60)
    return buf.getvalue()

# ---------------- WORDPRESS UPLOAD ---------------- #
def upload_wp(img_bytes, filename):
    url = f"{WP_BASE_URL}/wp-json/wp/v2/media"

    headers = {
        **get_auth(),
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": "image/jpeg"
    }

    try:
        res = requests.post(url, headers=headers, data=img_bytes, timeout=30)

        if res.status_code in [200, 201]:
            link = res.json().get("source_url")
            log(f"✅ Uploaded → {link}")
            return link
        else:
            log(f"❌ WP Error {res.status_code}: {res.text}")
            return None
    except Exception as e:
        log(f"❌ Upload Exception: {e}")
        return None

# ---------------- DB SAVE ---------------- #
def save_db(symbol, timeframe, url, date):
    conn = get_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO another_screenshot (symbol, timeframe, img_path, chart_date)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE img_path=VALUES(img_path)
        """, (symbol, timeframe, url, date))

        conn.commit()
        log(f"💾 Saved DB → {symbol}")
    except Exception as e:
        conn.rollback()
        log(f"❌ DB Error: {e}")
    finally:
        cur.close()
        conn.close()

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(options=opts)

def capture(symbol, date, url):
    driver = get_driver()
    try:
        log(f"🌐 Opening {symbol}")
        driver.get(url)
        time.sleep(8)

        wait = WebDriverWait(driver, 30)
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'chart')]")))

        ActionChains(driver).move_to_element(chart).click().perform()

        ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()

        inp = wait.until(EC.visibility_of_element_located((By.XPATH, "//input")))
        inp.send_keys(Keys.CONTROL + "a")
        inp.send_keys(Keys.BACKSPACE)
        inp.send_keys(date + Keys.ENTER)

        time.sleep(10)

        log("📸 Taking screenshot")
        img = chart.screenshot_as_png
        img = compress_image(img)

        filename = f"{symbol}.jpg"
        link = upload_wp(img, filename)

        if link:
            save_db(symbol, "day", link, date)

    except Exception as e:
        log(f"❌ Capture error: {e}")
    finally:
        driver.quit()

# ---------------- PROCESS ---------------- #
def process(row):
    log(f"DEBUG ROW → {row}")

    symbol = str(row.get("Symbol") or row.get("symbol") or "").strip()
    date = str(row.get("dates") or row.get("Date") or "").strip()
    url = str(row.get("Day") or row.get("day") or "").strip()

    if not symbol or not date or not url:
        log("⚠️ Skipped (missing data)")
        return

    capture(symbol, date, url)

# ---------------- MAIN ---------------- #
def main():
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)

    sheet = gc.open(SPREADSHEET_NAME).worksheet(TAB_NAME)
    data = sheet.get_all_records()

    log(f"📊 TOTAL ROWS: {len(data)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as ex:
        ex.map(process, data)

    log("🏁 Done")

if __name__ == "__main__":
    main()
