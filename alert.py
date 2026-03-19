import os
import time
import json
import random
import hashlib
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


# =========================================================
# CONFIG
# =========================================================
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

SAVE_DAY = True
SAVE_WEEK = True

CHROME_DRIVER_PATH = ChromeDriverManager().install()


# =========================================================
# HELPERS
# =========================================================
def log(msg):
    print(msg, flush=True)

def safe_str(v):
    if v is None:
        return ""
    return str(v).strip()

def safe_int(v, default=0):
    try:
        val = safe_str(v)
        if val == "":
            return default
        return int(float(val))
    except Exception:
        return default

def normalize_symbol(v):
    return safe_str(v).upper()

def clean_headers(header_list):
    return [safe_str(col) for col in header_list]

def deduplicate_columns(df, df_name="DataFrame"):
    duplicate_mask = df.columns.duplicated()
    if duplicate_mask.any():
        dupes = list(pd.Series(df.columns)[duplicate_mask])
        log(f"⚠️ Duplicate columns found in {df_name}: {dupes}. Keeping first occurrence only.")
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


# =========================================================
# DB CLASS
# =========================================================
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


# =========================================================
# SELENIUM
# =========================================================
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--lang=en-US")

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

        added = 0
        skipped = 0

        for c in cookies:
            name = c.get("name")
            value = c.get("value")

            if not name or value is None:
                skipped += 1
                continue

            cookie_domain = safe_str(c.get("domain")).lower()

            try:
                payload = {
                    "name": name,
                    "value": value,
                    "path": c.get("path", "/"),
                }

                if cookie_domain and "tradingview.com" in cookie_domain and cookie_domain != "tradingview.com":
                    payload["domain"] = cookie_domain

                if "expiry" in c and c["expiry"]:
                    try:
                        payload["expiry"] = int(c["expiry"])
                    except Exception:
                        pass

                if "secure" in c:
                    payload["secure"] = bool(c["secure"])

                if "httpOnly" in c:
                    payload["httpOnly"] = bool(c["httpOnly"])

                driver.add_cookie(payload)
                added += 1

            except Exception as cookie_err:
                log(f"⚠️ Skipping cookie {name}: {cookie_err}")
                skipped += 1

        driver.refresh()
        time.sleep(3)

        if added == 0:
            log("❌ No TradingView cookies could be added.")
            return False

        log(f"✅ TradingView cookies injected. Added={added}, Skipped={skipped}")
        return True

    except Exception as e:
        log(f"❌ Cookie injection failed: {e}")
        return False


# =========================================================
# GOOGLE SHEETS
# =========================================================
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
    df = pd.DataFrame(stock_raw[1:], columns=headers)
    df.columns = clean_headers(df.columns)
    df = deduplicate_columns(df, "stock list sheet")

    if df.shape[1] < 4:
        raise Exception("Stock list sheet must have at least 4 columns: Symbol, ?, Week URL, Day URL")

    df = df.copy()
    df["_symbol_norm"] = df.iloc[:, 0].apply(normalize_symbol)
    df["_week_url"] = df.iloc[:, 2].apply(safe_str)
    df["_day_url"] = df.iloc[:, 3].apply(safe_str)

    df = df[df["_symbol_norm"] != ""]

    symbol_map = {}
    for _, row in df.iterrows():
        sym = row["_symbol_norm"]
        if sym not in symbol_map:
            symbol_map[sym] = {
                "week": row["_week_url"],
                "day": row["_day_url"],
            }

    log("✅ Stock sheet loaded successfully.")
    log(f"✅ Total stock rows: {len(df)}")
    log(f"✅ Unique symbols mapped: {len(symbol_map)}")
    log(f"✅ Columns: {list(df.columns)}")

    return symbol_map


# =========================================================
# DB READ
# =========================================================
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


# =========================================================
# ALERT HELPERS
# =========================================================
def parse_alerts_json(raw_json, symbol, filter_id):
    raw_json = safe_str(raw_json)
    if not raw_json:
        return []

    try:
        data = json.loads(raw_json)
    except Exception as e:
        log(f"⚠️ Invalid alerts_json for symbol={symbol}, filter_id={filter_id}: {e}")
        return []

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    if isinstance(data, dict):
        return [data]

    log(f"⚠️ alerts_json is neither list nor object for symbol={symbol}, filter_id={filter_id}")
    return []

def alert_matches_condition(alert_obj):
    active_val = safe_int(alert_obj.get("active"))
    triggered_val = safe_int(alert_obj.get("triggered"))
    return active_val == 1 and triggered_val > 0

def explain_alert_condition(alert_obj):
    active_val = safe_int(alert_obj.get("active"))
    triggered_val = safe_int(alert_obj.get("triggered"))
    return f"active={active_val}, triggered={triggered_val}, needs active=1, triggered>0"


# =========================================================
# CHANGE HASH / DUPLICATE REDUCTION
# =========================================================
def build_change_hash(source_row, timeframe, alert_obj):
    payload = {
        "filter_id": safe_int(source_row.get("id")),
        "symbol": normalize_symbol(source_row.get("symbol")),
        "timeframe": safe_str(timeframe).lower(),

        "alert_id": safe_str(alert_obj.get("id")),
        "alert_type": safe_str(alert_obj.get("type")),
        "alert_email": safe_str(alert_obj.get("email")),
        "active": safe_int(alert_obj.get("active")),
        "triggered": safe_int(alert_obj.get("triggered")),
        "triggered_at": safe_str(alert_obj.get("triggered_at")),
        "created_at": safe_str(alert_obj.get("created_at")),

        "source_filter_type": safe_str(source_row.get("filter_type")),
        "source_timeframe": safe_str(source_row.get("timeframe")),
        "source_day": safe_int(source_row.get("day")),
        "source_last_shift_date": safe_str(source_row.get("last_shift_date")),
        "source_review_status": safe_str(source_row.get("review_status")),
        "source_review_reason": safe_str(source_row.get("review_reason")),
        "source_entry_price": safe_str(source_row.get("entry_price")),
        "source_action_date": safe_str(source_row.get("action_date")),
        "source_month_name": safe_str(source_row.get("month_name")),
        "source_week_label": safe_str(source_row.get("week_label")),

        "raw_alert": alert_obj,
    }

    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def get_last_saved_hash(db: DB, filter_id, symbol, timeframe, alert_id):
    query = f"""
        SELECT change_hash
        FROM `{TARGET_TABLE}`
        WHERE filter_id = %s
          AND symbol = %s
          AND timeframe = %s
          AND alert_id = %s
        ORDER BY id DESC
        LIMIT 1
    """

    try:
        conn = db.ensure()
        cur = conn.cursor()
        cur.execute(query, (filter_id, symbol, timeframe, alert_id))
        row = cur.fetchone()
        cur.close()
        return row[0] if row and row[0] else None
    except Exception as e:
        log(f"⚠️ Failed to fetch last change hash for {symbol} | {timeframe} | {alert_id}: {e}")
        return None

def has_state_changed(db: DB, source_row, timeframe, alert_obj):
    filter_id = safe_int(source_row.get("id"))
    symbol = normalize_symbol(source_row.get("symbol"))
    alert_id = safe_str(alert_obj.get("id"))

    new_hash = build_change_hash(source_row, timeframe, alert_obj)
    old_hash = get_last_saved_hash(db, filter_id, symbol, timeframe, alert_id)

    if old_hash == new_hash:
        return False, new_hash

    return True, new_hash


# =========================================================
# DB SAVE
# =========================================================
def save_alert_screenshot(db: DB, source_row, timeframe, alert_obj, image_data, change_hash):
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
            change_hash,
            screenshot
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        source_row.get("id"),
        normalize_symbol(source_row.get("symbol")),
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
        change_hash,
        image_data
    )

    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, values)
            cur.close()
            log(
                f"✅ Saved: symbol={normalize_symbol(source_row.get('symbol'))} | "
                f"timeframe={timeframe} | alert_type={safe_str(alert_obj.get('type'))} | "
                f"alert_id={safe_str(alert_obj.get('id'))}"
            )
            return True
        except Exception as e:
            log(f"⚠️ DB save error ({attempt + 1}/{DB_RETRY}): {e}")
            db.connect()
            time.sleep(1)

    log(f"❌ Failed to save screenshot for symbol={normalize_symbol(source_row.get('symbol'))} | timeframe={timeframe}")
    return False


# =========================================================
# SCREENSHOT
# =========================================================
def take_chart_screenshot(driver, url, symbol, timeframe):
    if not url:
        log(f"⚠️ Missing URL for {symbol} | {timeframe}")
        return None

    if "tradingview.com" not in url.lower():
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


# =========================================================
# MAIN PROCESS
# =========================================================
def process_alert_rows(driver, db, filter_rows, symbol_map):
    if not filter_rows:
        log("ℹ️ No rows found in filter table.")
        return

    total_alert_objects = 0
    total_matched_alerts = 0
    total_saved = 0
    total_skipped_duplicate = 0
    total_missing_symbol_url = 0

    for row in filter_rows:
        symbol = normalize_symbol(row.get("symbol"))
        filter_id = row.get("id")
        raw_json = row.get("alerts_json")

        if not symbol:
            log(f"⚠️ Skipping row with empty symbol | filter_id={filter_id}")
            continue

        alerts = parse_alerts_json(raw_json, symbol, filter_id)
        if not alerts:
            log(f"ℹ️ No valid alerts for symbol={symbol} | filter_id={filter_id}")
            continue

        total_alert_objects += len(alerts)

        matched_alerts = []
        for alert in alerts:
            log(
                f"🔎 Checking alert | symbol={symbol} | filter_id={filter_id} | "
                f"alert_id={safe_str(alert.get('id'))} | {explain_alert_condition(alert)}"
            )

            if alert_matches_condition(alert):
                matched_alerts.append(alert)

        if not matched_alerts:
            log(f"ℹ️ No matched alerts for symbol={symbol} | filter_id={filter_id}")
            continue

        total_matched_alerts += len(matched_alerts)
        log(f"🚀 Matched symbol={symbol} | matched alerts={len(matched_alerts)}")

        symbol_urls = symbol_map.get(symbol)
        if not symbol_urls:
            log(f"⚠️ Symbol not found in stock sheet: {symbol}")
            total_missing_symbol_url += len(matched_alerts)
            continue

        tasks = []
        if SAVE_DAY:
            tasks.append(("day", symbol_urls.get("day")))
        if SAVE_WEEK:
            tasks.append(("week", symbol_urls.get("week")))

        for alert_obj in matched_alerts:
            alert_id = safe_str(alert_obj.get("id"))

            for timeframe, url in tasks:
                if not url:
                    log(f"⚠️ URL missing for symbol={symbol} | timeframe={timeframe}")
                    total_missing_symbol_url += 1
                    continue

                changed, change_hash = has_state_changed(db, row, timeframe, alert_obj)

                if not changed:
                    log(
                        f"ℹ️ No change detected, skipping screenshot | "
                        f"symbol={symbol} | timeframe={timeframe} | alert_id={alert_id}"
                    )
                    total_skipped_duplicate += 1
                    continue

                image_data = take_chart_screenshot(driver, url, symbol, timeframe)
                if not image_data:
                    continue

                if save_alert_screenshot(db, row, timeframe, alert_obj, image_data, change_hash):
                    total_saved += 1

    log("=====================================================")
    log(f"✅ Total alert objects parsed: {total_alert_objects}")
    log(f"✅ Total matched alerts: {total_matched_alerts}")
    log(f"✅ Total screenshots saved: {total_saved}")
    log(f"✅ Total duplicate skips: {total_skipped_duplicate}")
    log(f"✅ Total missing symbol/url skips: {total_missing_symbol_url}")
    log("=====================================================")


# =========================================================
# MAIN
# =========================================================
def main():
    db = None
    driver = None

    try:
        log("🚀 Starting alert screenshot bot...")

        db = DB(DB_CONFIG)
        log("✅ Database connected.")

        client = get_gspread_client()
        symbol_map = load_stock_sheet(client)

        filter_rows = fetch_filter_rows(db)

        driver = get_driver()
        log("✅ Chrome driver started.")

        if not inject_tv_cookies(driver):
            raise Exception("TradingView cookie injection failed.")

        process_alert_rows(driver, db, filter_rows, symbol_map)

        log("🏁 Alert screenshot bot finished successfully.")

    except Exception as e:
        log(f"❌ Fatal error: {e}")

    finally:
        if driver:
            try:
                driver.quit()
                log("✅ Browser closed.")
            except Exception:
                pass

        if db:
            db.close()
            log("✅ Database connection closed.")


if __name__ == "__main__":
    main()
