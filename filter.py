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

TARGET_TABLE = "filter"
ALLOWED_TRIGGER_VALUES = [0, 1, 2, 3, 4]

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_str(v):
    if v is None:
        return ""
    return str(v).strip()

def safe_int(v):
    try:
        txt = str(v).strip()
        if txt == "":
            return -1
        return int(float(txt))
    except (ValueError, TypeError):
        return -1

def make_unique_headers(headers):
    seen = {}
    final_headers = []

    for h in headers:
        name = safe_str(h)
        if name == "":
            name = "blank_header"

        if name in seen:
            seen[name] += 1
            final_headers.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            final_headers.append(name)

    return final_headers

def get_first_matching_column(df, target_name):
    for col in df.columns:
        if safe_str(col) == target_name:
            return col
    return None

# ---------------- DB CLASS ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass

        self.conn = mysql.connector.connect(**self.config)
        self.conn.autocommit = True
        return self.conn

    def ensure(self):
        if not self.conn or not self.conn.is_connected():
            return self.connect()
        return self.conn

    def close(self):
        try:
            if self.conn:
                self.conn.close()
        except Exception:
            pass

def save_screenshot(db: DB, symbol, timeframe, filter_type, trigger_val, image):
    query = f"""
        INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, filter_type, day, screenshot)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            day = VALUES(day),
            created_at = CURRENT_TIMESTAMP
    """

    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, (symbol, timeframe, filter_type, trigger_val, image))
            cur.close()
            log(f"✅ Saved: {symbol} | {filter_type} | {timeframe} | Value: {trigger_val}")
            return True
        except Exception as e:
            log(f"⚠️ DB error on attempt {attempt + 1}: {e}")
            try:
                db.connect()
            except Exception as reconnect_error:
                log(f"⚠️ Reconnect failed: {reconnect_error}")
            time.sleep(1)

    return False

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("❌ TRADINGVIEW_COOKIES not found.")
            return False

        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(2)

        for c in cookies:
            driver.add_cookie({
                "name": c.get("name"),
                "value": c.get("value"),
                "domain": ".tradingview.com",
                "path": "/"
            })

        driver.refresh()
        time.sleep(2)
        return True

    except Exception as e:
        log(f"❌ Cookie injection error: {e}")
        return False

def capture_and_save(driver, db, symbol, filter_type, trigger_val, day_url, week_url):
    tasks = [("day", day_url), ("week", week_url)]

    for tf_name, url in tasks:
        if not url or "tradingview.com" not in url:
            log(f"⚠️ Invalid URL for {symbol} | {tf_name}")
            continue

        try:
            driver.get(url)

            chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//div[contains(@class,'chart-container')]")
                )
            )

            time.sleep(POST_LOAD_SLEEP)
            image = chart.screenshot_as_png

            save_screenshot(
                db=db,
                symbol=symbol,
                timeframe=tf_name,
                filter_type=filter_type,
                trigger_val=trigger_val,
                image=image
            )

        except Exception as e:
            log(f"❌ Screenshot failed for {symbol} | {filter_type} | {tf_name}: {e}")

# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        creds = os.getenv("GSPREAD_CREDENTIALS")
        if not creds:
            log("❌ GSPREAD_CREDENTIALS not found.")
            return

        client = gspread.service_account_from_dict(json.loads(creds))

        # ---------------- FETCH MV2 SHEET ---------------- #
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        if not mv2_raw or len(mv2_raw) < 2:
            log("❌ MV2 sheet is empty.")
            return

        original_mv2_headers = mv2_raw[0]
        unique_mv2_headers = make_unique_headers(original_mv2_headers)
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=unique_mv2_headers)

        if len(original_mv2_headers) != len(set([safe_str(h) for h in original_mv2_headers])):
            log("⚠️ Duplicate headers found in MV2 sheet.")
            log(f"Original headers: {original_mv2_headers}")
            log(f"Using unique headers: {unique_mv2_headers}")

        # ---------------- FETCH STOCK URL SHEET ---------------- #
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        if not stock_raw or len(stock_raw) < 2:
            log("❌ Stock list sheet is empty.")
            return

        stock_headers = make_unique_headers(stock_raw[0])
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_headers)

        # Column 0 = symbol, col 2 = week URL, col 3 = day URL
        week_urls = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))
        day_urls = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 3].astype(str).str.strip()
        ))

        # ---------------- FIND TRIGGER COLUMNS SAFELY ---------------- #
        dtrigger_col = get_first_matching_column(df_mv2, "D_Trigger")
        dtrigger_s_col = get_first_matching_column(df_mv2, "D_Trigger_S")

        if not dtrigger_col:
            log("❌ Column 'D_Trigger' not found in MV2 sheet.")
            return

        if not dtrigger_s_col:
            log("❌ Column 'D_Trigger_S' not found in MV2 sheet.")
            return

        log(f"✅ Using D_Trigger column: {dtrigger_col}")
        log(f"✅ Using D_Trigger_S column: {dtrigger_s_col}")

        # First column assumed as symbol column
        symbol_col = df_mv2.columns[0]
        log(f"✅ Using symbol column: {symbol_col}")

        # ---------------- CLEAN NUMERIC VALUES ---------------- #
        df_mv2["D_Trigger_num"] = df_mv2[dtrigger_col].apply(safe_int)
        df_mv2["D_Trigger_S_num"] = df_mv2[dtrigger_s_col].apply(safe_int)

        # ---------------- START SELENIUM ---------------- #
        driver = get_driver()
        if not inject_tv_cookies(driver):
            log("❌ Cookie injection failed.")
            return

        # =====================================================
        # PART 1: D_Trigger
        # Save if D_Trigger in [0,1,2,3,4]
        # =====================================================
        log(f"🔍 Scanning D_Trigger for values: {ALLOWED_TRIGGER_VALUES}")

        dtrigger_rows = df_mv2[df_mv2["D_Trigger_num"].isin(ALLOWED_TRIGGER_VALUES)]
        log(f"✅ Total D_Trigger matched rows: {len(dtrigger_rows)}")

        for _, row in dtrigger_rows.iterrows():
            symbol = safe_str(row[symbol_col])
            dtrigger_val = row["D_Trigger_num"]

            if not symbol:
                continue

            day_url = day_urls.get(symbol, "")
            week_url = week_urls.get(symbol, "")

            log(f"🚀 D_Trigger matched: {symbol} | Value: {dtrigger_val}")

            capture_and_save(
                driver=driver,
                db=db,
                symbol=symbol,
                filter_type="D_Trigger",
                trigger_val=dtrigger_val,
                day_url=day_url,
                week_url=week_url
            )

        # =====================================================
        # PART 2: D_Trigger_S
        # Save if D_Trigger_S in [0,1,2,3,4]
        # and D_Trigger_S != D_Trigger
        # =====================================================
        log(f"🔍 Scanning D_Trigger_S for values: {ALLOWED_TRIGGER_VALUES} with D_Trigger_S != D_Trigger")

        dtrigger_s_rows = df_mv2[
            (df_mv2["D_Trigger_S_num"].isin(ALLOWED_TRIGGER_VALUES)) &
            (df_mv2["D_Trigger_S_num"] != df_mv2["D_Trigger_num"])
        ]
        log(f"✅ Total D_Trigger_S matched rows: {len(dtrigger_s_rows)}")

        for _, row in dtrigger_s_rows.iterrows():
            symbol = safe_str(row[symbol_col])
            dtrigger_s_val = row["D_Trigger_S_num"]
            dtrigger_val = row["D_Trigger_num"]

            if not symbol:
                continue

            day_url = day_urls.get(symbol, "")
            week_url = week_urls.get(symbol, "")

            log(f"🚀 D_Trigger_S matched: {symbol} | D_Trigger_S: {dtrigger_s_val} | D_Trigger: {dtrigger_val}")

            capture_and_save(
                driver=driver,
                db=db,
                symbol=symbol,
                filter_type="D_Trigger_S",
                trigger_val=dtrigger_s_val,
                day_url=day_url,
                week_url=week_url
            )

        log("🏁 All triggers processed successfully.")

    except Exception as e:
        log(f"❌ Fatal error: {e}")

    finally:
        try:
            if driver:
                driver.quit()
        except Exception:
            pass

        db.close()

if __name__ == "__main__":
    main()
