import os
import time
import json
import random
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

SOURCE_TABLE = "filter"
TARGET_TABLE = "filter_alert_screenshots"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
GSHEET_RETRY = 5

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
        val = safe_str(v)
        if val == "":
            return 0
        return int(float(val))
    except:
        return 0

def clean_headers(header_list):
    return [safe_str(col) for col in header_list]

def deduplicate_columns(df, df_name="DataFrame"):
    if df.columns.duplicated().any():
        log(f"⚠️ Duplicate columns found in {df_name}, keeping first occurrence only.")
    return df.loc[:, ~df.columns.duplicated()]

def retry_gsheet_call(fn, label="Google Sheets call", max_retry=GSHEET_RETRY):
    last_error = None

    for attempt in range(1, max_retry + 1):
        try:
            return fn()
        except Exception as e:
            last_error = e
            wait_time = min((2 ** (attempt - 1)) + random.uniform(0.5, 1.5), 20)
            log(f"⚠️ {label} failed (attempt {attempt}/{max_retry}): {e}")
            if attempt < max_retry:
                log(f"⏳ Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)

    raise Exception(f"{label} failed after {max_retry} attempts: {last_error}")


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
            except:
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
        except:
            pass


# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("⚠️ TRADINGVIEW_COOKIES env variable missing.")
            return False

        cookies = json.loads(cookie_data)
        if not isinstance(cookies, list) or len(cookies) == 0:
            log("⚠️ TRADINGVIEW_COOKIES is empty or invalid.")
            return False

        driver.get("https://www.tradingview.com/")
        time.sleep(2)

        for c in cookies:
            name = c.get("name")
            value = c.get("value")
            if not name or value is None:
                continue

            try:
                cookie_payload = {
                    "name": name,
                    "value": value,
                    "domain": c.get("domain", ".tradingview.com"),
                    "path": c.get("path", "/"),
                }

                if "expiry" in c and c["expiry"]:
                    try:
                        cookie_payload["expiry"] = int(c["expiry"])
                    except:
                        pass

                if "secure" in c:
                    cookie_payload["secure"] = bool(c["secure"])

                if "httpOnly" in c:
                    cookie_payload["httpOnly"] = bool(c["httpOnly"])

                driver.add_cookie(cookie_payload)

            except Exception as cookie_err:
                log(f"⚠️ Skipping cookie {name}: {cookie_err}")

        driver.refresh()
        time.sleep(3)
        log("✅ TradingView cookies injected.")
        return True

    except Exception as e:
        log(f"❌ Cookie injection failed: {e}")
        return False


# ---------------- GSHEET ---------------- #
def get_gspread_client():
    creds = os.getenv("GSPREAD_CREDENTIALS")
    if not creds:
        raise Exception("GSPREAD_CREDENTIALS env variable missing.")

    return retry_gsheet_call(
        lambda: gspread.service_account_from_dict(json.loads(creds)),
        label="Google auth"
    )

def load_stock_sheet(client):
    stock_ws = retry_gsheet_call(
        lambda: client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID),
        label="Open stock worksheet"
    )

    stock_raw = retry_gsheet_call(
        lambda: stock_ws.get_all_values(),
        label="Read stock worksheet values"
    )

    if not stock_raw or len(stock_raw) < 2:
        raise Exception("Stock list sheet is empty or invalid.")

    headers = clean_headers(stock_raw[0])
    df_stocks = pd.DataFrame(stock_raw[1:], columns=headers)
    df_stocks.columns = clean_headers(df_stocks.columns)
    df_stocks = deduplicate_columns(df_stocks, "stock list sheet")

    if df_stocks.shape[1] < 4:
        raise Exception("Stock list sheet must have at least 4 columns: Symbol, ?, Week URL, Day URL")

    log("✅ Stock sheet loaded successfully.")
    log(f"✅ Total stock rows: {len(df_stocks)}")
    log(f"✅ Columns: {list(df_stocks.columns)}")

    return df_stocks


# ---------------- DB READ ---------------- #
def fetch_filter_rows(db: DB):
    query = f"""
        SELECT
            id,
            symbol,
            timeframe,
            filter_type,
            day,
            last_shift_date,
            review_status,
            review_reason,
            entry_price,
            action_date,
            month_name,
            week_label,
            alerts_json
        FROM `{SOURCE_TABLE}`
        WHERE alerts_json IS NOT NULL
          AND TRIM(alerts_json) <> ''
    """

    conn = db.ensure()
    cur = conn.cursor(dictionary=True)
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    log(f"✅ Loaded {len(rows)} rows from `{SOURCE_TABLE}` having alerts_json.")
    return rows


# ---------------- DUPLICATE CHECK ---------------- #
def already_saved(db: DB, filter_id, symbol, timeframe, alert_id, triggered):
    query = f"""
        SELECT id
        FROM `{TARGET_TABLE}`
        WHERE filter_id = %s
          AND symbol = %s
          AND timeframe = %s
          AND alert_id = %s
          AND triggered = %s
        LIMIT 1
    """

    try:
        conn = db.ensure()
        cur = conn.cursor()
        cur.execute(query, (filter_id, symbol, timeframe, alert_id, triggered))
        row = cur.fetchone()
        cur.close()
        return row is not None
    except Exception as e:
        log(f"⚠️ Duplicate check failed for {symbol} | {timeframe} | {alert_id}: {e}")
        return False


# ---------------- DB SAVE ---------------- #
def save_alert_screenshot(db: DB, source_row, timeframe, alert_obj, image_data):
    query = f"""
        INSERT INTO `{TARGET_TABLE}` (
            filter_id,
            symbol,
            timeframe,
            alert_id,
            alert_type,
            alert_email,
            active,
            triggered,
            alert_created_at,
            alert_triggered_at,
            source_filter_type,
            source_timeframe,
            source_day,
            source_last_shift_date,
            source_review_status,
            source_review_reason,
            source_entry_price,
            source_action_date,
            source_month_name,
            source_week_label,
            raw_alert_json,
            screenshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        source_row.get("id"),
        safe_str(source_row.get("symbol")),
        timeframe,
        safe_str(alert_obj.get("id")) or None,
        safe_str(alert_obj.get("type")) or None,
        safe_str(alert_obj.get("email")) or None,
        safe_int(alert_obj.get("active")),
        safe_int(alert_obj.get("triggered")),
        safe_str(alert_obj.get("created_at")) or None,
        safe_str(alert_obj.get("triggered_at")) or None,
        safe_str(source_row.get("filter_type")) or None,
        safe_str(source_row.get("timeframe")) or None,
        safe_int(source_row.get("day")),
        source_row.get("last_shift_date"),
        safe_str(source_row.get("review_status")) or None,
        safe_str(source_row.get("review_reason")) or None,
        source_row.get("entry_price"),
        source_row.get("action_date"),
        safe_str(source_row.get("month_name")) or None,
        safe_str(source_row.get("week_label")) or None,
        json.dumps(alert_obj, ensure_ascii=False),
        image_data
    )

    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, values)
            cur.close()
            log(f"✅ Saved: {source_row.get('symbol')} | {timeframe} | {alert_obj.get('type')} | triggered={alert_obj.get('triggered')}")
            return True
        except Exception as e:
            log(f"⚠️ DB save error ({attempt + 1}/{DB_RETRY}) for {source_row.get('symbol')} | {timeframe}: {e}")
            db.connect()
            time.sleep(1)

    log(f"❌ Failed to save: {source_row.get('symbol')} | {timeframe}")
    return False


# ---------------- SCREENSHOT ---------------- #
def take_chart_screenshot(driver, url, symbol, timeframe):
    if not url:
        log(f"⚠️ Missing URL for {symbol} | {timeframe}")
        return None

    if "tradingview.com" not in url:
        log(f"⚠️ Invalid TradingView URL for {symbol} | {timeframe}: {url}")
        return None

    try:
        driver.get(url)

        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
            EC.visibility_of_element_located(
                (By.XPATH, "//div[contains(@class,'chart-container')]")
            )
        )

        time.sleep(POST_LOAD_SLEEP)
        image_data = chart.screenshot_as_png

        if not image_data:
            log(f"⚠️ Empty screenshot for {symbol} | {timeframe}")
            return None

        return image_data

    except Exception as e:
        log(f"❌ Screenshot failed for {symbol} | {timeframe}: {e}")
        return None


# ---------------- ALERT PROCESS ---------------- #
def process_alert_rows(driver, db, filter_rows, day_urls, week_urls):
    if not filter_rows:
        log("ℹ️ No rows found in filter table.")
        return

    total_matched_alerts = 0
    total_saved = 0

    for row in filter_rows:
        symbol = safe_str(row.get("symbol"))
        alerts_json = safe_str(row.get("alerts_json"))

        if not symbol or not alerts_json:
            continue

        try:
            alerts = json.loads(alerts_json)
        except Exception as e:
            log(f"⚠️ Invalid alerts_json for symbol={symbol}, filter_id={row.get('id')}: {e}")
            continue

        if not isinstance(alerts, list):
            log(f"⚠️ alerts_json is not a list for symbol={symbol}, filter_id={row.get('id')}")
            continue

        matched_alerts = []
        for alert in alerts:
            if not isinstance(alert, dict):
                continue

            active_val = safe_int(alert.get("active"))
            triggered_val = safe_int(alert.get("triggered"))

            # Condition:
            # active == 1 and triggered > 1
            if active_val == 1 and triggered_val > 1:
                matched_alerts.append(alert)

        if not matched_alerts:
            continue

        log(f"🚀 Matched symbol: {symbol} | alerts matched: {len(matched_alerts)}")
        total_matched_alerts += len(matched_alerts)

        for alert_obj in matched_alerts:
            alert_id = safe_str(alert_obj.get("id"))
            triggered_val = safe_int(alert_obj.get("triggered"))

            tasks = [
                ("day", day_urls.get(symbol)),
                ("week", week_urls.get(symbol))
            ]

            for tf_name, url in tasks:
                if already_saved(db, row.get("id"), symbol, tf_name, alert_id, triggered_val):
                    log(f"ℹ️ Already saved, skipping: {symbol} | {tf_name} | {alert_id} | triggered={triggered_val}")
                    continue

                image_data = take_chart_screenshot(driver, url, symbol, tf_name)
                if not image_data:
                    continue

                if save_alert_screenshot(db, row, tf_name, alert_obj, image_data):
                    total_saved += 1

    log(f"✅ Total matched alerts: {total_matched_alerts}")
    log(f"✅ Total screenshots saved: {total_saved}")


# ---------------- MAIN ---------------- #
def main():
    db = None
    driver = None

    try:
        log("🚀 Starting alert screenshot bot...")

        # DB
        db = DB(DB_CONFIG)
        log("✅ Database connected.")

        # Google auth + stock sheet
        client = get_gspread_client()
        df_stocks = load_stock_sheet(client)

        # Column mapping:
        # column 0 = symbol
        # column 2 = week URL
        # column 3 = day URL
        symbol_series = df_stocks.iloc[:, 0].astype(str).str.strip()
        week_series = df_stocks.iloc[:, 2].astype(str).str.strip()
        day_series = df_stocks.iloc[:, 3].astype(str).str.strip()

        week_urls = dict(zip(symbol_series, week_series))
        day_urls = dict(zip(symbol_series, day_series))

        log(f"✅ URL map prepared for {len(day_urls)} symbols.")

        # Filter rows
        filter_rows = fetch_filter_rows(db)

        # Selenium
        driver = get_driver()
        log("✅ Chrome driver started.")

        if not inject_tv_cookies(driver):
            raise Exception("TradingView cookie injection failed.")

        # Process rows
        process_alert_rows(driver, db, filter_rows, day_urls, week_urls)

        log("🏁 Alert screenshot bot finished successfully.")

    except Exception as e:
        log(f"❌ Fatal error: {e}")

    finally:
        if driver:
            try:
                driver.quit()
                log("✅ Browser closed.")
            except:
                pass

        if db:
            db.close()
            log("✅ Database connection closed.")


if __name__ == "__main__":
    main()
