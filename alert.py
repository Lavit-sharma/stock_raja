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
    df = df.loc[:, ~df.columns.duplicated()]
    return df


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
                driver.add_cookie({
                    "name": name,
                    "value": value,
                    "domain": ".tradingview.com",
                    "path": "/"
                })
            except Exception as cookie_err:
                log(f"⚠️ Skipping cookie {name}: {cookie_err}")

        driver.refresh()
        time.sleep(2)
        log("✅ TradingView cookies injected.")
        return True

    except Exception as e:
        log(f"❌ Cookie injection failed: {e}")
        return False


# ---------------- SHEET LOADER ---------------- #
def load_stock_sheet(client):
    stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
    stock_raw = stock_ws.get_all_values()

    if not stock_raw or len(stock_raw) < 2:
        raise Exception("Stock list sheet is empty or invalid.")

    headers = clean_headers(stock_raw[0])
    df_stocks = pd.DataFrame(stock_raw[1:], columns=headers)
    df_stocks.columns = clean_headers(df_stocks.columns)
    df_stocks = deduplicate_columns(df_stocks, "stock list sheet")

    if df_stocks.shape[1] < 4:
        raise Exception("Stock list sheet must have at least 4 columns: Symbol, ?, Week URL, Day URL")

    log("✅ Stock sheet loaded.")
    log(list(df_stocks.columns))

    return df_stocks


# ---------------- FETCH FILTER ROWS ---------------- #
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

    log(f"✅ Loaded {len(rows)} rows from `{SOURCE_TABLE}` with alerts_json.")
    return rows


# ---------------- SAVE SCREENSHOT ---------------- #
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
            source_day,
            source_last_shift_date,
            source_review_status,
            source_month_name,
            source_week_label,
            raw_alert_json,
            screenshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        source_row.get("id"),
        safe_str(source_row.get("symbol")),
        timeframe,
        safe_str(alert_obj.get("id")),
        safe_str(alert_obj.get("type")),
        safe_str(alert_obj.get("email")),
        safe_int(alert_obj.get("active")),
        safe_int(alert_obj.get("triggered")),
        safe_str(alert_obj.get("created_at")) or None,
        safe_str(alert_obj.get("triggered_at")) or None,
        safe_str(source_row.get("filter_type")) or None,
        safe_int(source_row.get("day")),
        source_row.get("last_shift_date"),
        safe_str(source_row.get("review_status")) or None,
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
            log(f"✅ Saved alert screenshot: {source_row.get('symbol')} | {timeframe} | {alert_obj.get('type')}")
            return
        except Exception as e:
            log(f"⚠️ DB save error ({attempt + 1}/{DB_RETRY}) for {source_row.get('symbol')} {timeframe}: {e}")
            db.connect()
            time.sleep(1)

    log(f"❌ Failed to save screenshot for {source_row.get('symbol')} | {timeframe}")


# ---------------- PROCESS ALERTS ---------------- #
def process_alert_rows(driver, db, filter_rows, day_urls, week_urls):
    if not filter_rows:
        log("ℹ️ No filter rows found.")
        return

    for row in filter_rows:
        symbol = safe_str(row.get("symbol"))
        alerts_json = safe_str(row.get("alerts_json"))

        if not symbol or not alerts_json:
            continue

        try:
            alerts = json.loads(alerts_json)
        except Exception as e:
            log(f"⚠️ Invalid JSON for symbol {symbol}, row id {row.get('id')}: {e}")
            continue

        if not isinstance(alerts, list):
            log(f"⚠️ alerts_json is not a list for symbol {symbol}, row id {row.get('id')}")
            continue

        matched_alerts = []
        for alert in alerts:
            if not isinstance(alert, dict):
                continue

            active_val = safe_int(alert.get("active"))
            triggered_val = safe_int(alert.get("triggered"))

            # Your requested condition:
            # active = 1 and triggered > 1
            if active_val == 1 and triggered_val > 1:
                matched_alerts.append(alert)

        if not matched_alerts:
            continue

        log(f"🚀 Symbol matched: {symbol} | matched alerts: {len(matched_alerts)}")

        # For every matched alert, save both day and week screenshots if URL exists
        for alert_obj in matched_alerts:
            tasks = [
                ("day", day_urls.get(symbol)),
                ("week", week_urls.get(symbol))
            ]

            for tf_name, url in tasks:
                if not url:
                    log(f"⚠️ Missing URL for {symbol} | {tf_name}")
                    continue

                if "tradingview.com" not in url:
                    log(f"⚠️ Invalid TradingView URL for {symbol} | {tf_name}: {url}")
                    continue

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
                        log(f"⚠️ Empty screenshot for {symbol} | {tf_name}")
                        continue

                    save_alert_screenshot(db, row, tf_name, alert_obj, image_data)

                except Exception as e:
                    log(f"❌ Screenshot failed for {symbol} | {tf_name} | alert {alert_obj.get('id')}: {e}")


# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        # Google auth
        creds = os.getenv("GSPREAD_CREDENTIALS")
        if not creds:
            raise Exception("GSPREAD_CREDENTIALS env variable missing.")

        client = gspread.service_account_from_dict(json.loads(creds))

        # load stock sheet
        df_stocks = load_stock_sheet(client)

        # symbol -> urls
        # Column 0 = Symbol, Column 2 = Week URL, Column 3 = Day URL
        symbol_series = df_stocks.iloc[:, 0].astype(str).str.strip()
        week_series = df_stocks.iloc[:, 2].astype(str).str.strip()
        day_series = df_stocks.iloc[:, 3].astype(str).str.strip()

        week_urls = dict(zip(symbol_series, week_series))
        day_urls = dict(zip(symbol_series, day_series))

        # fetch filter table rows
        filter_rows = fetch_filter_rows(db)

        # selenium + cookies
        driver = get_driver()
        if not inject_tv_cookies(driver):
            log("❌ Stopping because TradingView cookie injection failed.")
            return

        # process
        process_alert_rows(driver, db, filter_rows, day_urls, week_urls)

        log("🏁 Alert screenshot process completed successfully.")

    except Exception as e:
        log(f"❌ Fatal error: {e}")

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        db.close()


if __name__ == "__main__":
    main()
