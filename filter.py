import os
import time
import json
import gspread
import pandas as pd
import mysql.connector

from collections import Counter, defaultdict
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
MAX_DAY_TO_KEEP = 4 

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_str(v):
    if v is None: return ""
    return str(v).strip()

def safe_float(v):
    try:
        val = safe_str(v)
        if not val or val == "#N/A" or val == "None": return -1.0
        return float(val.replace(',', ''))
    except (ValueError, TypeError):
        return -1.0

def clean_headers(header_list):
    return [safe_str(col) for col in header_list]

def deduplicate_columns(df):
    return df.loc[:, ~df.columns.duplicated()]

def get_column_case_insensitive(df, target_name):
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
            try: self.conn.close()
            except: pass
        self.conn = mysql.connector.connect(**self.config)
        self.conn.autocommit = True
        return self.conn

    def ensure(self):
        if not self.conn or not self.conn.is_connected():
            return self.connect()
        return self.conn

    def close(self):
        try:
            if self.conn: self.conn.close()
        except: pass

def roll_days_forward(db: DB):
    update_query = f"UPDATE `{TARGET_TABLE}` SET `day` = `day` + 0"
    delete_query = f"DELETE FROM `{TARGET_TABLE}` WHERE `day` > %s AND LOWER(TRIM(COALESCE(`review_status`, ''))) = 'rejected'"
    try:
        conn = db.ensure()
        cur = conn.cursor()
        cur.execute(update_query)
        cur.execute(delete_query, (MAX_DAY_TO_KEEP,))
        cur.close()
        log("✅ Day rollover complete.")
    except Exception as e:
        log(f"⚠️ Rollover error: {e}")

def save_screenshot(db: DB, symbol, timeframe, filter_type, image):
    query = f"INSERT IGNORE INTO `{TARGET_TABLE}` (`symbol`, `timeframe`, `filter_type`, `day`, `screenshot`) VALUES (%s, %s, %s, 0, %s)"
    try:
        conn = db.ensure()
        cur = conn.cursor()
        cur.execute(query, (symbol, timeframe, filter_type, image))
        if cur.rowcount > 0:
            log(f"✅ Saved: {symbol} | {filter_type} | {timeframe}")
        cur.close()
    except Exception as e:
        log(f"⚠️ DB save error: {e}")

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        return True
    except: return False

# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        roll_days_forward(db)
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        # Load Data
        df_mv2 = pd.DataFrame(client.open_by_url(MV2_SQL_URL).sheet1.get_all_values())
        df_mv2.columns = clean_headers(df_mv2.iloc[0])
        df_mv2 = deduplicate_columns(df_mv2.iloc[1:])

        df_stocks = pd.DataFrame(client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID).get_all_values())
        df_stocks.columns = clean_headers(df_stocks.iloc[0])
        df_stocks = df_stocks.iloc[1:]
        
        day_urls = dict(zip(df_stocks.iloc[:,0].astype(str).str.strip(), df_stocks.iloc[:,3].astype(str).str.strip()))
        week_urls = dict(zip(df_stocks.iloc[:,0].astype(str).str.strip(), df_stocks.iloc[:,2].astype(str).str.strip()))

        # Get Columns
        mx_col = get_column_case_insensitive(df_mv2, "MXMN")
        mxl_col = get_column_case_insensitive(df_mv2, "MXMN_low")
        dt_col = get_column_case_insensitive(df_mv2, "D_Trigger")
        ef1_col = get_column_case_insensitive(df_mv2, "D_EF1")
        ef2_col = get_column_case_insensitive(df_mv2, "D_EF2")
        dcl_col = get_column_case_insensitive(df_mv2, "D_CLABOVE")

        # Convert Values
        df_mv2["mx_v"] = df_mv2[mx_col].apply(safe_float)
        df_mv2["mxl_v"] = df_mv2[mxl_col].apply(safe_float)
        df_mv2["dt_v"] = df_mv2[dt_col].apply(safe_float)
        df_mv2["ef1_v"] = df_mv2[ef1_col].apply(safe_float)
        df_mv2["ef2_v"] = df_mv2[ef2_col].apply(safe_float)
        df_mv2["dcl_v"] = df_mv2[dcl_col].apply(safe_float)

        # PRE-CALCULATE TRIGGERS (Stock -> List of Filters)
        # This prevents opening the same URL multiple times
        stock_to_filters = defaultdict(list)

        # Logic for Conso Filter
        conso_mask = ((df_mv2["mx_v"] < 15) & (df_mv2["dt_v"] > 0) & (df_mv2["ef1_v"]/df_mv2["dt_v"] < 2)) | \
                     ((df_mv2["mx_v"] < 15) & (df_mv2["dt_v"] > 0) & (df_mv2["ef2_v"]/df_mv2["dt_v"] < 2))
        for s in df_mv2[conso_mask].iloc[:, 0].astype(str).str.strip():
            stock_to_filters[s].append("Conso Filter")

        # Logic for Moment Filter
        moment_mask = ((df_mv2["mxl_v"] < 10) & (df_mv2["dt_v"] > 0) & (df_mv2["ef1_v"]/df_mv2["dt_v"] < 2)) | \
                      ((df_mv2["mxl_v"] < 10) & (df_mv2["dt_v"] > 0) & (df_mv2["ef2_v"]/df_mv2["dt_v"] < 2))
        for s in df_mv2[moment_mask].iloc[:, 0].astype(str).str.strip():
            stock_to_filters[s].append("Moment Filter")

        # Logic for Jump Filter
        jump_mask = ((df_mv2["dcl_v"] > 3) & (df_mv2["dt_v"] > 0) & (df_mv2["ef2_v"]/df_mv2["dt_v"] < 2)) | \
                    ((df_mv2["dcl_v"] > 3) & (df_mv2["dt_v"] > 0) & (df_mv2["ef1_v"]/df_mv2["dt_v"] < 2))
        for s in df_mv2[jump_mask].iloc[:, 0].astype(str).str.strip():
            stock_to_filters[s].append("Jump Filter")

        if not stock_to_filters:
            log("ℹ️ No stocks matched any filters today.")
            return

        driver = get_driver()
        if not inject_tv_cookies(driver): return

        log(f"🔍 Found {len(stock_to_filters)} unique stocks to capture.")

        # PROCESS EACH UNIQUE STOCK ONCE
        for symbol, filters in stock_to_filters.items():
            for tf_name, url_dict in [("day", day_urls), ("week", week_urls)]:
                url = url_dict.get(symbol)
                if url and "tradingview.com" in url:
                    try:
                        driver.get(url)
                        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                        )
                        time.sleep(POST_LOAD_SLEEP)
                        screenshot = chart.screenshot_as_png
                        
                        # Save the single screenshot for all applicable filters
                        for filter_label in filters:
                            save_screenshot(db, symbol, tf_name, filter_label, screenshot)
                            
                    except Exception as e:
                        log(f"❌ Screenshot failed for {symbol} ({tf_name}): {e}")

        log("🏁 All triggers processed.")

    except Exception as e: log(f"❌ Fatal error: {e}")
    finally:
        if driver: driver.quit()
        db.close()

if __name__ == "__main__":
    main()
