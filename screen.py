import os
import time
import json
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
}

DAILY_THRESHOLD = 0.07
MONTHLY_THRESHOLD = 0.25

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6

DB_RETRY = 3
PAGE_RETRY = 2

CHROME_DRIVER_PATH = ChromeDriverManager().install()


# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)


def safe_float(v):
    try:
        return float(str(v).replace('%', '').strip())
    except:
        return 0.0


def safe_str(v):
    try:
        return str(v).strip()
    except:
        return ""


# ✅ DB CONNECT + AUTO-RECONNECT WRAPPER
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        try:
            if self.conn:
                try:
                    self.conn.close()
                except:
                    pass
        except:
            pass

        self.conn = mysql.connector.connect(**self.config)
        self.conn.autocommit = True
        return self.conn

    def ensure(self):
        try:
            if self.conn is None:
                return self.connect()
            if not self.conn.is_connected():
                return self.connect()
            return self.conn
        except:
            return self.connect()

    def close(self):
        try:
            if self.conn:
                self.conn.close()
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
            log(f"⚠️ DB save failed {symbol}({timeframe}) attempt {attempt}/{DB_RETRY}: {e}")
            try:
                db.connect()
            except:
                pass
            time.sleep(1.5)
        finally:
            try:
                if cur:
                    cur.close()
            except:
                pass

    log(f"❌ DB save failed permanently for {symbol}({timeframe}): {last_err}")
    return False


# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)

    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
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
            driver.get(url)
            return True
        except Exception as e:
            log(f"⚠️ Page load failed attempt {i}/{retries}: {e}")
            time.sleep(2)
    return False


# ---------------- MAIN ---------------- #
def main():
    log(f"🔎 DB TARGET {DB_CONFIG['host']} / {DB_CONFIG['database']}")

    db = DB(DB_CONFIG)
    clear_db_before_run(db)

    # ---- Sheets ----
    try:
        creds = os.getenv("GSPREAD_CREDENTIALS")
        if not creds:
            log("❌ GSPREAD_CREDENTIALS missing.")
            return

        client = gspread.service_account_from_dict(json.loads(creds))

        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        week_url_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))
        day_url_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 3].astype(str).str.strip()
        ))

        log(f"✅ Loaded MV2 rows: {len(df_mv2)}")
        log(f"✅ Loaded StockList rows (gid={STOCK_LIST_GID}): {len(df_stocks)}")

    except Exception as e:
        log(f"❌ Sheet Error: {e}")
        return

    # ---- Browser ----
    driver = get_driver()
    try:
        if not inject_tv_cookies(driver):
            return

        qualified = 0
        saved = 0

        N_IDX = 13
        AL_IDX = 37
        mv2_headers = list(df_mv2.columns)

        for _, row in df_mv2.iterrows():
            symbol = ""
            try:
                symbol = safe_str(row.iloc[0])
                sector = safe_str(row.iloc[1]).upper()
                if not symbol:
                    continue

                if sector in ("INDICES", "MUTUAL FUND SCHEME"):
                    continue

                daily_val = safe_float(row.iloc[14])
                monthly_val = safe_float(row.iloc[15])

                # ✅ If EITHER condition is met, we process BOTH URLs
                if (daily_val >= DAILY_THRESHOLD or monthly_val >= MONTHLY_THRESHOLD):
                    qualified += 1

                    n_al_map = {}
                    max_i = min(AL_IDX, len(mv2_headers) - 1)
                    for i in range(N_IDX, max_i + 1):
                        key = safe_str(mv2_headers[i]) or f"col_{i}"
                        n_al_map[key] = safe_str(row.iloc[i])
                    mv2_n_al_json = json.dumps(n_al_map, ensure_ascii=False)

                    day_url = day_url_map.get(symbol)
                    week_url = week_url_map.get(symbol)

                    # --- Step 1: Capture Daily URL (StockList Column D) ---
                    if day_url and "tradingview.com" in day_url:
                        if open_with_retry(driver, day_url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            if save_to_mysql(db, symbol, "daily", chart.screenshot_as_png, mv2_n_al_json):
                                saved += 1
                    else:
                        log(f"⚠️ Missing Day URL for {symbol}")

                    # --- Step 2: Capture Weekly URL (StockList Column C) ---
                    if week_url and "tradingview.com" in week_url:
                        if open_with_retry(driver, week_url, retries=PAGE_RETRY):
                            chart = wait_chart(driver)
                            time.sleep(POST_LOAD_SLEEP)
                            if save_to_mysql(db, symbol, "weekly", chart.screenshot_as_png, mv2_n_al_json):
                                saved += 1
                    else:
                        log(f"⚠️ Missing Week URL for {symbol}")

            except Exception as e:
                log(f"⚠️ Screenshot error {symbol}: {e}")

        log(f"✅ QUALIFIED SYMBOLS: {qualified}")
        log(f"✅ TOTAL SCREENSHOTS SAVED: {saved}")

    finally:
        try:
            driver.quit()
        except:
            pass
        db.close()


if __name__ == "__main__":
    main()
