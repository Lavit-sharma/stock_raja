import os
import time
import json
import gspread
import pandas as pd
import mysql.connector

from collections import Counter
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

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
MAX_DAY_TO_KEEP = 4  # keep day 0 to day 4 = total 5 days

# webdriver-manager installs chrome driver automatically
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
        if not val:
            return -1
        return int(float(val))
    except (ValueError, TypeError):
        return -1

def clean_headers(header_list):
    return [safe_str(col) for col in header_list]

def deduplicate_columns(df, df_name="DataFrame"):
    """
    Removes duplicate column names, keeping first occurrence.
    """
    counts = Counter(df.columns)
    duplicates = {k: v for k, v in counts.items() if v > 1 and k != ""}

    if duplicates:
        log(f"⚠️ Duplicate headers found in {df_name}: {duplicates}")
        log(f"⚠️ Keeping first occurrence only in {df_name}.")

    blank_cols = [i for i, c in enumerate(df.columns) if safe_str(c) == ""]
    if blank_cols:
        log(f"⚠️ Blank header columns found in {df_name} at positions: {blank_cols}")

    df = df.loc[:, ~df.columns.duplicated()]
    return df

def get_column_case_insensitive(df, target_name):
    """
    Finds actual column name ignoring case and spaces.
    """
    target = safe_str(target_name).lower()
    for col in df.columns:
        if safe_str(col).lower() == target:
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


# ---------------- DAILY ROLLOVER ---------------- #
def roll_days_forward(db: DB):
    """
    Runs once per script execution.
    Shifts day values and deletes rows older than day 4.
    """
    update_query = f"UPDATE `{TARGET_TABLE}` SET `day` = `day` + 0"
    delete_query = f"DELETE FROM `{TARGET_TABLE}` WHERE `day` > %s"

    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()

            cur.execute(update_query)
            cur.execute(delete_query, (MAX_DAY_TO_KEEP,))

            cur.close()
            log("✅ Day rollover completed.")
            return
        except Exception as e:
            log(f"⚠️ Rollover error (attempt {attempt + 1}/{DB_RETRY}): {e}")
            db.connect()
            time.sleep(1)

    raise Exception("Failed to complete day rollover after retries.")


# ---------------- SAVE SCREENSHOT ---------------- #
def save_screenshot(db: DB, symbol, timeframe, filter_type, image):
    query = f"""
        INSERT INTO `{TARGET_TABLE}` (`symbol`, `timeframe`, `filter_type`, `day`, `screenshot`)
        VALUES (%s, %s, %s, 0, %s)
    """
    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, (symbol, timeframe, filter_type, image))
            cur.close()
            log(f"✅ Saved: {symbol} | {filter_type} | {timeframe} | day=0")
            return
        except Exception as e:
            log(f"⚠️ DB save error for {symbol} {timeframe} {filter_type} (attempt {attempt + 1}/{DB_RETRY}): {e}")
            db.connect()
            time.sleep(1)

    log(f"❌ Failed to save after retries: {symbol} | {timeframe} | {filter_type}")


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


# ---------------- SHEET LOADERS ---------------- #
def load_mv2_sheet(client):
    mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()

    if not mv2_raw or len(mv2_raw) < 2:
        raise Exception("MV2 sheet is empty or invalid.")

    headers = clean_headers(mv2_raw[0])
    df_mv2 = pd.DataFrame(mv2_raw[1:], columns=headers)
    df_mv2.columns = clean_headers(df_mv2.columns)
    df_mv2 = deduplicate_columns(df_mv2, "MV2 sheet")

    log("✅ MV2 columns loaded:")
    log(list(df_mv2.columns))

    return df_mv2

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

    log("✅ Stock sheet columns loaded:")
    log(list(df_stocks.columns))

    return df_stocks


# ---------------- SCREENSHOT PROCESSOR ---------------- #
def process_trigger_rows(driver, db, rows_df, day_urls, week_urls, filter_type, log_message):
    log(log_message)

    if rows_df.empty:
        log(f"ℹ️ No rows matched for {filter_type}")
        return

    for _, row in rows_df.iterrows():
        symbol = safe_str(row.iloc[0])
        if not symbol:
            continue

        log(f"🚀 Triggered: {symbol} ({filter_type})")

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

                save_screenshot(db, symbol, tf_name, filter_type, image_data)

            except Exception as e:
                log(f"❌ Screenshot failed for {symbol} | {tf_name} | {filter_type}: {e}")


# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        # Step 1: shift old records
        roll_days_forward(db)

        # Step 2: Google auth
        creds = os.getenv("GSPREAD_CREDENTIALS")
        if not creds:
            raise Exception("GSPREAD_CREDENTIALS env variable missing.")

        client = gspread.service_account_from_dict(json.loads(creds))

        # Step 3: load sheets
        df_mv2 = load_mv2_sheet(client)
        df_stocks = load_stock_sheet(client)

        # Step 4: symbol -> urls
        # Column 0 = Symbol, Column 2 = Week URL, Column 3 = Day URL
        symbol_series = df_stocks.iloc[:, 0].astype(str).str.strip()
        week_series = df_stocks.iloc[:, 2].astype(str).str.strip()
        day_series = df_stocks.iloc[:, 3].astype(str).str.strip()

        week_urls = dict(zip(symbol_series, week_series))
        day_urls = dict(zip(symbol_series, day_series))

        # Step 5: selenium + cookies
        driver = get_driver()
        if not inject_tv_cookies(driver):
            log("❌ Stopping because TradingView cookie injection failed.")
            return

        # Step 6: find required columns safely
        d_trigger_col = get_column_case_insensitive(df_mv2, "D_Trigger")
        d_trigger_s_col = get_column_case_insensitive(df_mv2, "D_Trigger_S")
        w_trigger_col = get_column_case_insensitive(df_mv2, "W_Trigger")
        w_trigger_s_col = get_column_case_insensitive(df_mv2, "W_Trigger_S")
        cr1_col = get_column_case_insensitive(df_mv2, "CR1")
        cr2_col = get_column_case_insensitive(df_mv2, "CR2")

        required_map = {
            "D_Trigger": d_trigger_col,
            "D_Trigger_S": d_trigger_s_col,
            "W_Trigger": w_trigger_col,
            "W_Trigger_S": w_trigger_s_col,
            "CR1": cr1_col,
            "CR2": cr2_col,
        }

        for expected_name, actual_name in required_map.items():
            if not actual_name:
                log(f"⚠️ Header '{expected_name}' not found in Google Sheet.")
                return

        # Step 7: convert to numeric helper columns
        df_mv2["D_Trigger_num"] = df_mv2[d_trigger_col].apply(safe_int)
        df_mv2["D_Trigger_S_num"] = df_mv2[d_trigger_s_col].apply(safe_int)
        df_mv2["W_Trigger_num"] = df_mv2[w_trigger_col].apply(safe_int)
        df_mv2["W_Trigger_S_num"] = df_mv2[w_trigger_s_col].apply(safe_int)
        df_mv2["CR1_num"] = df_mv2[cr1_col].apply(safe_int)
        df_mv2["CR2_num"] = df_mv2[cr2_col].apply(safe_int)

        # -------------------------
        # PART 1: D_Trigger
        # Condition: D_Trigger == 0
        # -------------------------
        dtrigger_rows = df_mv2[df_mv2["D_Trigger_num"] == 0]
        process_trigger_rows(
            driver,
            db,
            dtrigger_rows,
            day_urls,
            week_urls,
            "D_Trigger",
            "🔍 Scanning D_Trigger for value 0"
        )

        # -------------------------
        # PART 2: D_Trigger_S
        # Condition:
        # D_Trigger_S == 0
        # AND D_Trigger_S != D_Trigger
        # -------------------------
        dtrigger_s_rows = df_mv2[
            (df_mv2["D_Trigger_S_num"] == 0) &
            (df_mv2["D_Trigger_S_num"] != df_mv2["D_Trigger_num"])
        ]
        process_trigger_rows(
            driver,
            db,
            dtrigger_s_rows,
            day_urls,
            week_urls,
            "D_Trigger_S",
            "🔍 Scanning D_Trigger_S for value 0 with D_Trigger_S != D_Trigger"
        )

        # -------------------------
        # PART 3: W_Trigger
        # Condition: W_Trigger == 0
        # -------------------------
        wtrigger_rows = df_mv2[df_mv2["W_Trigger_num"] == 0]
        process_trigger_rows(
            driver,
            db,
            wtrigger_rows,
            day_urls,
            week_urls,
            "W_Trigger",
            "🔍 Scanning W_Trigger for value 0"
        )

        # -------------------------
        # PART 4: W_Trigger_S
        # Condition:
        # W_Trigger_S == 0
        # AND W_Trigger_S != W_Trigger
        # -------------------------
        wtrigger_s_rows = df_mv2[
            (df_mv2["W_Trigger_S_num"] == 0) &
            (df_mv2["W_Trigger_S_num"] != df_mv2["W_Trigger_num"])
        ]
        process_trigger_rows(
            driver,
            db,
            wtrigger_s_rows,
            day_urls,
            week_urls,
            "W_Trigger_S",
            "🔍 Scanning W_Trigger_S for value 0 with W_Trigger_S != W_Trigger"
        )

        # -------------------------
        # PART 5: CR1
        # Condition: CR1 == 1
        # Same logic as D_Trigger / W_Trigger
        # -------------------------
        cr1_rows = df_mv2[df_mv2["CR1_num"] == 1]
        process_trigger_rows(
            driver,
            db,
            cr1_rows,
            day_urls,
            week_urls,
            "CR1",
            "🔍 Scanning CR1 for value 1"
        )

        # -------------------------
        # PART 6: CR2
        # Condition:
        # CR2 == 1
        # AND CR2 != CR1
        # Same logic as D_Trigger_S / W_Trigger_S
        # -------------------------
        cr2_rows = df_mv2[
            (df_mv2["CR2_num"] == 1) &
            (df_mv2["CR2_num"] != df_mv2["CR1_num"])
        ]
        process_trigger_rows(
            driver,
            db,
            cr2_rows,
            day_urls,
            week_urls,
            "CR2",
            "🔍 Scanning CR2 for value 1 with CR2 != CR1"
        )

        log("🏁 All triggers processed successfully.")

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
