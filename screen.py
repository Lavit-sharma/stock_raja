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
POST_LOAD_SLEEP = 6
CHART_WAIT_SEC = 30

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_float(v):
    try: return float(str(v).replace('%', '').strip())
    except: return 0.0

def safe_str(v):
    try: return str(v).strip()
    except: return ""

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
        if self.conn is None or not self.conn.is_connected():
            return self.connect()
        return self.conn

    def close(self):
        if self.conn: self.conn.close()

def save_to_mysql(db, symbol, timeframe_label, image, mv2_json):
    """
    timeframe_label will now be: daily-daily, daily-month, week-daily, week-month
    """
    query = """
        INSERT INTO stock_screenshots (symbol, timeframe, screenshot, mv2_n_al)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            mv2_n_al = VALUES(mv2_n_al),
            created_at = CURRENT_TIMESTAMP
    """
    try:
        conn = db.ensure()
        cur = conn.cursor()
        cur.execute(query, (symbol, timeframe_label, image, mv2_json))
        log(f"✅ Saved {symbol} as [{timeframe_label}]")
        cur.close()
    except Exception as e:
        log(f"❌ DB Error for {symbol}: {e}")

# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver

def inject_cookies(driver):
    try:
        data = os.getenv("TRADINGVIEW_COOKIES")
        if not data: return False
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        for c in json.loads(data):
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        time.sleep(4)
        return True
    except: return False

def capture_and_save(driver, db, symbol, url, label, mv2_json):
    try:
        driver.get(url)
        # Wait for the specific chart container
        WebDriverWait(driver, CHART_WAIT_SEC).until(EC.visibility_of_element_located((By.CLASS_NAME, "chart-container")))
        time.sleep(POST_LOAD_SLEEP)
        chart = driver.find_element(By.CLASS_NAME, "chart-container")
        save_to_mysql(db, symbol, label, chart.screenshot_as_png, mv2_json)
        return True
    except Exception as e:
        log(f"⚠️ Capture failed for {symbol} ({label}): {e}")
        return False

# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    
    try:
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))
        
        # Load Sheets
        mv2_sheet = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        headers = mv2_sheet[0]
        df_mv2 = pd.DataFrame(mv2_sheet[1:], columns=headers)
        
        df_stocks = pd.DataFrame(client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID).get_all_values())
        
        # Mapping URLs from StockList (Col A=Symbol, Col C=Week, Col D=Day)
        week_urls = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 2].str.strip()))
        day_urls = dict(zip(df_stocks.iloc[:, 0].str.strip(), df_stocks.iloc[:, 3].str.strip()))
        
        log(f"✅ Sheets Loaded. Symbols in MV2: {len(df_mv2)}")
    except Exception as e:
        log(f"❌ Initialization Error: {e}"); return

    driver = get_driver()
    if not inject_cookies(driver): 
        log("❌ Cookie injection failed"); return

    for _, row in df_mv2.iterrows():
        symbol = safe_str(row.iloc[0])
        sector = safe_str(row.iloc[1]).upper()
        if not symbol or sector in ("INDICES", "MUTUAL FUND SCHEME"): continue

        daily_val = safe_float(row.iloc[14])   # Col O
        monthly_val = safe_float(row.iloc[15]) # Col P

        # Pre-build the JSON metadata
        meta = {headers[i]: safe_str(row.iloc[i]) for i in range(13, min(37, len(headers)))}
        mv2_json = json.dumps(meta, ensure_ascii=False)

        day_url = day_urls.get(symbol)
        week_url = week_urls.get(symbol)

        # --- TRIGGER 1: DAILY 7% TARGET ---
        if daily_val >= DAILY_THRESHOLD:
            log(f"🚀 {symbol} hit DAILY trigger ({daily_val})")
            if day_url and "tradingview" in day_url:
                capture_and_save(driver, db, symbol, day_url, "daily-daily", mv2_json)
            if week_url and "tradingview" in week_url:
                capture_and_save(driver, db, symbol, week_url, "week-daily", mv2_json)

        # --- TRIGGER 2: MONTHLY 25% TARGET ---
        if monthly_val >= MONTHLY_THRESHOLD:
            log(f"🚀 {symbol} hit MONTHLY trigger ({monthly_val})")
            if day_url and "tradingview" in day_url:
                capture_and_save(driver, db, symbol, day_url, "daily-month", mv2_json)
            if week_url and "tradingview" in week_url:
                capture_and_save(driver, db, symbol, week_url, "week-month", mv2_json)

    driver.quit()
    db.close()
    log("🏁 Process Finished!")

if __name__ == "__main__":
    main()
