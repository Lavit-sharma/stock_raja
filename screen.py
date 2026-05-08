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

DAILY_THRESHOLD = 0.07
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

        if s == "":
            return 0.0

        s = s.replace('%', '')
        s = s.replace(',', '')
        s = s.replace('−', '-')
        s = s.replace('–', '-')
        s = s.replace('—', '-')
        s = s.replace('+', '')
        s = s.replace('₹', '')
        s = s.strip()

        match = re.search(r'-?\d*\.?\d+', s)
        if match:
            return float(match.group())

        return 0.0
    except:
        return 0.0


def safe_str(v):
    try:
        return str(v).strip()
    except:
        return ""


# ---------------- DB WRAPPER ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        last_err = None

        try:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
        except:
            pass

        for attempt in range(1, DB_CONNECT_RETRY + 1):
            try:
                log(f"📡 Connecting to MySQL... attempt {attempt}/{DB_CONNECT_RETRY}")

                self.conn = mysql.connector.connect(
                    host=self.config["host"],
                    user=self.config["user"],
                    password=self.config["password"],
                    database=self.config["database"],
                    port=self.config["port"],
                    connection_timeout=15,
                    autocommit=True
                )

                self.conn.autocommit = True
                log("✅ MySQL connected.")
                return self.conn

            except Exception as e:
                last_err = e
                log(f"⚠️ MySQL connection failed attempt {attempt}/{DB_CONNECT_RETRY}: {e}")

                if attempt < DB_CONNECT_RETRY:
                    log(f"⏳ Waiting {DB_CONNECT_WAIT}s before retry...")
                    time.sleep(DB_CONNECT_WAIT)

        raise RuntimeError(f"MySQL connection failed after {DB_CONNECT_RETRY} attempts: {last_err}")

    def ensure(self):
        try:
            if self.conn is None:
                return self.connect()

            if not self.conn.is_connected():
                return self.connect()

            return self.conn
        except Exception as e:
            log(f"⚠️ DB ensure reconnect needed: {e}")
            return self.connect()

    def close(self):
        try:
            if self.conn:
                self.conn.close()
                log("🔌 DB closed.")
        except:
            pass


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
        try:
            if cur:
                cur.close()
        except:
            pass


def save_to_mysql(db: DB, symbol, timeframe, image, mv2_n_al_json):
    query = """
        INSERT INTO stock_screenshots
            (symbol, timeframe, screenshot, mv2_n_al)
        VALUES
            (%s, %s, %s, %s)
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
            try:
                db.connect()
            except Exception as reconnect_err:
                log(f"⚠️ DB reconnect failed during save: {reconnect_err}")
            time.sleep(2)
        finally:
            try:
                if cur:
                    cur.close()
            except:
                pass

    log(f"❌ DB save failed permanently for {symbol} ({timeframe}): {last_err}")
    return False


# ---------------- SELENIUM ---------------- #
def get_driver():
    log("🌐 Installing ChromeDriver...")
    chrome_driver_path = ChromeDriverManager().install()
    log("✅ ChromeDriver installed.")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    service = Service(chrome_driver_path)

    log("🚀 Launching Chrome browser...")
    driver = webdriver.Chrome(service=service, options=opts)
    log("✅ Browser started.")

    driver.execute_script("""
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    })
    """)

    driver.set_page_load_timeout(60)
    return driver


def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("❌ TRADINGVIEW_COOKIES missing.")
            return False

        cookies = json.loads(cookie_data)

        driver.get("https://www.tradingview.com/")
        time.sleep(3)

        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"),
                    "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"),
                    "path": c.get("path", "/")
                })
            except:
                pass

        driver.refresh()
        time.sleep(4)
        log("✅ Cookies injected")
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
            log(f"🌍 Opening URL attempt {i}/{retries}: {url}")
            driver.get(url)
            return True
        except Exception as e:
            log(f"⚠️ Page load failed attempt {i}/{retries}: {e}")
            time.sleep(2)
    return False


# ---------------- MAIN ---------------- #
def main():
    log(f"🔎 DB TARGET {DB_CONFIG['host']} / {DB_CONFIG['database']}:{DB_CONFIG['port']}")

    db = None
    driver = None

    try:
        log("STEP 1: Creating DB object...")
        db = DB(DB_CONFIG)

        log("STEP 2: Clearing DB...")
        clear_db_before_run(db)

        log("STEP 3: Loading Google Sheets...")
        creds = os.getenv("GSPREAD_CREDENTIALS")
        if not creds:
            log("❌ GSPREAD_CREDENTIALS missing.")
            return

        client = gspread.service_account_from_dict(json.loads(creds))

        log("📄 Loading MV2 sheet...")
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
        log(f"✅ MV2 rows loaded: {len(df_mv2)}")

        log("📄 Loading Stock List sheet...")
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        log(f"✅ Stock list rows loaded: {len(df_stocks)}")

        week_url_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))

        day_url_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 3].astype(str).str.strip()
        ))

        log("STEP 4: Starting browser...")
        driver = get_driver()

        log("STEP 5: Injecting TradingView cookies...")
        if not inject_tv_cookies(driver):
            return

        mv2_headers = list(df_mv2.columns)

        if len(mv2_headers) > 14:
            log(f"📌 Daily value column = index 14 = {mv2_headers[14]}")
        if len(mv2_headers) > 15:
            log(f"📌 Monthly value column = index 15 = {mv2_headers[15]}")

        log("STEP 6: Processing rows...")

        for _, row in df_mv2.iterrows():
            symbol = ""
            try:
                symbol = safe_str(row.iloc[0])
                sector = safe_str(row.iloc[1]).upper()

                if not symbol or sector in ("INDICES", "MUTUAL FUND SCHEME"):
                    continue

                raw_daily = row.iloc[14] if len(row) > 14 else ""
                raw_monthly = row.iloc[15] if len(row) > 15 else ""

                daily_val = safe_float(raw_daily)
                monthly_val = safe_float(raw_monthly)

                log(
                    f"🔍 {symbol} | daily_raw=[{raw_daily}] parsed={daily_val} "
                    f"| monthly_raw=[{raw_monthly}] parsed={monthly_val}"
                )

                n_al_map = {
                    safe_str(mv2_headers[i]): safe_str(row.iloc[i])
                    for i in range(13, min(37, len(mv2_headers)))
                }
                mv2_n_al_json = json.dumps(n_al_map, ensure_ascii=False)

                day_url = day_url_map.get(symbol)
                week_url = week_url_map.get(symbol)

                # DAILY trigger
                if daily_val >= DAILY_THRESHOLD:
                    log(f"✅ DAILY TRIGGER: {symbol} ({daily_val} >= {DAILY_THRESHOLD})")

                    if day_url and "tradingview.com" in day_url:
                        if open_with_retry(driver, day_url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            save_to_mysql(db, symbol, "daily-daily", chart.screenshot_as_png, mv2_n_al_json)

                    if week_url and "tradingview.com" in week_url:
                        if open_with_retry(driver, week_url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            save_to_mysql(db, symbol, "week-daily", chart.screenshot_as_png, mv2_n_al_json)

                # MONTHLY trigger
                if monthly_val >= MONTHLY_THRESHOLD:
                    log(f"✅ MONTHLY TRIGGER: {symbol} ({monthly_val} >= {MONTHLY_THRESHOLD})")

                    if day_url and "tradingview.com" in day_url:
                        if open_with_retry(driver, day_url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            save_to_mysql(db, symbol, "daily-month", chart.screenshot_as_png, mv2_n_al_json)

                    if week_url and "tradingview.com" in week_url:
                        if open_with_retry(driver, week_url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            save_to_mysql(db, symbol, "week-month", chart.screenshot_as_png, mv2_n_al_json)

            except Exception as e:
                log(f"⚠️ Error processing {symbol}: {e}")

        log("🏁 DONE!")

    except Exception as e:
        log(f"❌ FATAL ERROR: {e}")
        log("🛑 Script stopped cleanly.")
        sys.exit(1)

    finally:
        try:
            if driver:
                driver.quit()
                log("🛑 Browser closed.")
        except:
            pass

        try:
            if db:
                db.close()
        except:
            pass


if __name__ == "__main__":
    main()
