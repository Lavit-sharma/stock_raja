import os
import time
import json
import re
import sys
import gspread
import pandas as pd
import mysql.connector

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843
MV2_SQL_URL = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

MONTHLY_THRESHOLD = 0.25
CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
DB_CONNECT_RETRY = 3
DB_CONNECT_WAIT = 5
PAGE_RETRY = 2

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_float(v):
    try:
        s = str(v).strip()
        if s == "": return 0.0
        s = s.replace('%', '').replace(',', '').replace('−', '-').replace('–', '-').replace('—', '-').replace('+', '').replace('₹', '').strip()
        match = re.search(r'-?\d*\.?\d+', s)
        return float(match.group()) if match else 0.0
    except:
        return 0.0

def safe_str(v):
    try: return str(v).strip()
    except: return ""

# ---------------- DB WRAPPER ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        last_err = None
        if self.conn:
            try: self.conn.close()
            except: pass

        for attempt in range(1, DB_CONNECT_RETRY + 1):
            try:
                log(f"📡 Connecting to MySQL... attempt {attempt}/{DB_CONNECT_RETRY}")
                self.conn = mysql.connector.connect(
                    **self.config,
                    connection_timeout=20,
                    autocommit=True
                )
                # Increase session limits to prevent "Connection Lost"
                cur = self.conn.cursor()
                cur.execute("SET SESSION wait_timeout = 28800")
                cur.execute("SET SESSION interactive_timeout = 28800")
                cur.execute("SET GLOBAL max_allowed_packet = 1073741824")
                cur.close()
                log("✅ MySQL connected.")
                return self.conn
            except Exception as e:
                last_err = e
                log(f"⚠️ MySQL connection failed: {e}")
                if attempt < DB_CONNECT_RETRY: time.sleep(DB_CONNECT_WAIT)

        raise RuntimeError(f"MySQL failed after {DB_CONNECT_RETRY} attempts: {last_err}")

    def ensure(self):
        try:
            if not self.conn or not self.conn.is_connected():
                return self.connect()
            self.conn.ping(reconnect=True, attempts=3, delay=2)
            return self.conn
        except:
            return self.connect()

    def close(self):
        try:
            if self.conn:
                self.conn.close()
                log("🔌 DB closed.")
        except: pass

def clear_db_before_run(db: DB):
    cur = None
    try:
        conn = db.ensure()
        cur = conn.cursor()
        log("🧹 Clearing old database entries...")
        cur.execute("TRUNCATE TABLE stock_screenshots")
        log("✅ Database is clean.")
    except Exception as e:
        log(f"❌ Error clearing database: {e}")
        raise
    finally:
        if cur: cur.close()

def save_to_mysql(db: DB, symbol, timeframe, image, mv2_n_al_json):
    query = """
        INSERT INTO stock_screenshots (symbol, timeframe, screenshot, mv2_n_al)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            mv2_n_al = VALUES(mv2_n_al),
            created_at = CURRENT_TIMESTAMP
    """
    last_err = None
    for attempt in range(1, DB_RETRY + 1):
        cur = None
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, (symbol, timeframe, image, mv2_n_al_json))
            log(f"✅ [DB] Saved {symbol} ({timeframe})")
            return True
        except Exception as e:
            last_err = e
            log(f"⚠️ DB save failed {symbol} ({timeframe}) attempt {attempt}/{DB_RETRY}: {e}")
            db.connect()
            time.sleep(2)
        finally:
            if cur: cur.close()
    return False

# ---------------- SELENIUM ---------------- #
def get_driver():
    chrome_driver_path = ChromeDriverManager().install()
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(chrome_driver_path), options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_page_load_timeout(60)
    return driver

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES2")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"), "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"), "path": c.get("path", "/")
                })
            except: pass
        driver.refresh()
        time.sleep(4)
        return True
    except Exception as e:
        log(f"❌ Cookie error: {e}")
        return False

def wait_chart(driver):
    return WebDriverWait(driver, CHART_WAIT_SEC).until(
        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
    )

def open_with_retry(driver, url, retries=2):
    for i in range(1, retries + 1):
        try:
            driver.get(url)
            return True
        except Exception as e:
            log(f"⚠️ Page load failed: {e}")
            time.sleep(2)
    return False

# ---------------- MAIN ---------------- #
def main():
    db = None
    driver = None
    try:
        db = DB(DB_CONFIG)
        clear_db_before_run(db)

        creds = os.getenv("GSPREAD_CREDENTIALS")
        if not creds: return
        client = gspread.service_account_from_dict(json.loads(creds))

        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        df_stocks = pd.DataFrame(stock_ws.get_all_values()[1:])
        
        week_url_map = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 2].str.strip()))
        day_url_map = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 3].str.strip()))

        driver = get_driver()
        if not inject_tv_cookies(driver): return

        mv2_headers = list(df_mv2.columns)

        for _, row in df_mv2.iterrows():
            symbol = safe_str(row.iloc[0])
            sector = safe_str(row.iloc[1]).upper()

            if not symbol or sector in ("INDICES", "MUTUAL FUND SCHEME"): continue

            monthly_val = safe_float(row.iloc[15] if len(row) > 15 else "")
            
            if monthly_val >= MONTHLY_THRESHOLD:
                log(f"✅ MONTHLY TRIGGER: {symbol} ({monthly_val})")
                
                n_al_map = {safe_str(mv2_headers[i]): safe_str(row.iloc[i]) for i in range(13, min(37, len(mv2_headers)))}
                mv2_json = json.dumps(n_al_map, ensure_ascii=False)

                urls = [("daily-month", day_url_map.get(symbol)), ("week-month", week_url_map.get(symbol))]
                for label, url in urls:
                    if url and "tradingview.com" in url:
                        if open_with_retry(driver, url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            save_to_mysql(db, symbol, label, chart.screenshot_as_png, mv2_json)

        log("🏁 DONE!")
    except Exception as e:
        log(f"❌ FATAL ERROR: {e}")
        sys.exit(1)
    finally:
        if driver: driver.quit()
        if db: db.close()

if __name__ == "__main__":
    main()
