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
MAX_DAY_TO_KEEP = 4

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_int(v):
    try:
        if v is None or str(v).strip() == "": return -1
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return -1

def fix_duplicate_columns(df):
    """Renames duplicate columns to ensure unique indexing."""
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols[cols == dup].index.values.tolist()] = [
            f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))
        ]
    df.columns = cols
    return df

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
        if self.conn: self.conn.close()

# ---------------- CORE LOGIC ---------------- #
def roll_days_forward(db: DB):
    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(f"UPDATE `{TARGET_TABLE}` SET `day` = `day` + 1")
            cur.execute(f"DELETE FROM `{TARGET_TABLE}` WHERE `day` > %s AND LOWER(TRIM(COALESCE(`review_status`, ''))) = 'rejected'", (MAX_DAY_TO_KEEP,))
            log(f"✅ Rollover: {cur.rowcount} rows cleaned.")
            cur.close()
            return
        except Exception as e:
            log(f"⚠️ Rollover retry {attempt+1}: {e}")
            db.connect()
            time.sleep(1)

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def main():
    db = DB(DB_CONFIG)
    driver = None
    try:
        roll_days_forward(db)
        
        # 1. Load Data
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))
        
        # Load MV2 and Stock Sheet
        mv2_sheet = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_sheet[1:], columns=[c.strip() for c in mv2_sheet[0]])
        df_mv2 = fix_duplicate_columns(df_mv2)
        
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID).get_all_values()
        df_stocks = pd.DataFrame(stock_ws[1:], columns=[c.strip() for c in stock_ws[0]])
        df_stocks = fix_duplicate_columns(df_stocks)

        # Create URL Map (Symbol -> {day: url, week: url})
        url_map = {row[0].strip(): {'week': row[2].strip(), 'day': row[3].strip()} for row in stock_ws[1:] if row[0]}

        # 2. Process Filters
        cols_to_fix = ["D_Trigger", "D_Trigger_S", "W_Trigger", "W_Trigger_S", "MXMN", "D_CLABOVE"]
        for col in cols_to_fix:
            if col in df_mv2.columns:
                df_mv2[f"{col}_n"] = df_mv2[col].apply(safe_int)

        # Define Filter Conditions with safety checks
        triggers = {
            "D_Trigger": df_mv2[df_mv2.get("D_Trigger_n", pd.Series([-1]*len(df_mv2))) == 0],
            "D_Trigger_S": df_mv2[(df_mv2.get("D_Trigger_S_n", -1) == 0) & (df_mv2.get("D_Trigger_S_n", -1) != df_mv2.get("D_Trigger_n", -1))],
            "W_Trigger": df_mv2[df_mv2.get("W_Trigger_n", -1) == 1],
            "W_Trigger_S": df_mv2[(df_mv2.get("W_Trigger_S_n", -1) == 0) & (df_mv2.get("W_Trigger_S_n", -1) != df_mv2.get("W_Trigger_n", -1))],
            "CONSO COUNT": df_mv2[(df_mv2.get("MXMN_n", 999) < 20) & (df_mv2.get("D_CLABOVE_n", -1) > 3)]
        }

        # 3. Setup Browser
        driver = get_driver()
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if cookie_data:
            driver.get("https://www.tradingview.com/")
            for c in json.loads(cookie_data):
                try: driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
                except: continue
            driver.refresh()

        # 4. Execute Screenshots
        for filter_name, matched_df in triggers.items():
            if matched_df.empty: continue
            log(f"🚀 Processing {filter_name}: {len(matched_df)} stocks found.")
            
            for _, row in matched_df.iterrows():
                symbol = str(row.iloc[0]).strip()
                urls = url_map.get(symbol)
                if not urls: continue

                for tf in ['day', 'week']:
                    url = urls[tf]
                    if "tradingview.com" not in url: continue
                    
                    try:
                        driver.get(url)
                        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                        )
                        time.sleep(POST_LOAD_SLEEP)
                        img = chart.screenshot_as_png
                        
                        # Direct DB Insert
                        conn = db.ensure()
                        cur = conn.cursor()
                        cur.execute(f"INSERT INTO `{TARGET_TABLE}` (symbol, timeframe, filter_type, day, screenshot) VALUES (%s, %s, %s, 0, %s)",
                                    (symbol, tf, filter_name, img))
                        cur.close()
                        log(f"   ✅ Saved {symbol} ({tf})")
                    except Exception as e:
                        log(f"   ❌ Error {symbol} {tf}: {e}")

        log("🏁 Execution Finished.")

    except Exception as e: log(f"❌ Fatal: {e}")
    finally:
        if driver: driver.quit()
        db.close()

if __name__ == "__main__":
    main()
