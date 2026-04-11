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
        if not val: return -1.0
        return float(val)
    except (ValueError, TypeError):
        return -1.0

def clean_headers(header_list):
    return [safe_str(col) for col in header_list]

# ---------------- MAIN ---------------- #
def main():
    # Database Connection
    db_conn = mysql.connector.connect(**DB_CONFIG)
    db_conn.autocommit = True
    
    driver = None
    
    try:
        # 1. Google Auth & Load Sheets
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json:
            raise Exception("GSPREAD_CREDENTIALS env variable missing.")
            
        client = gspread.service_account_from_dict(json.loads(creds_json))

        # Load MV2 Sheet
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=clean_headers(mv2_raw[0]))
        df_mv2 = df_mv2.loc[:, ~df_mv2.columns.duplicated()]
        
        # Load Stock List Sheet
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=clean_headers(stock_raw[0]))

        # Map URLs to Symbols
        symbol_col = df_stocks.columns[0]
        week_url_col = df_stocks.columns[2]
        day_url_col = df_stocks.columns[3]
        week_urls = dict(zip(df_stocks[symbol_col].str.strip(), df_stocks[week_url_col].str.strip()))
        day_urls = dict(zip(df_stocks[symbol_col].str.strip(), df_stocks[day_url_col].str.strip()))

        # 2. Map Column Names (Case-Insensitive)
        def find_col(name):
            for c in df_mv2.columns:
                if c.lower() == name.lower(): return c
            return None

        c_mxmn_low = find_col("MXMN_low")
        c_trigger = find_col("D_Trigger")
        c_ef1 = find_col("D_EF1")
        c_ef2 = find_col("D_EF2")
        c_mxmn = find_col("MXMN")
        c_clabove = find_col("D_CLABOVE")

        # 3. Apply Moment Filter Logic
        def check_moment(row):
            mxmn_low_val = safe_float(row.get(c_mxmn_low))
            trigger_val = safe_float(row.get(c_trigger))
            ef1_val = safe_float(row.get(c_ef1))
            ef2_val = safe_float(row.get(c_ef2))

            # Condition: MXMN_low < 10
            if mxmn_low_val == -1.0 or mxmn_low_val >= 10:
                return False

            # Prevent Division by Zero: if trigger is 0, we assume ratio condition is met
            if trigger_val <= 0:
                return True

            # Calculate Ratios
            ratio1 = ef1_val / trigger_val
            ratio2 = ef2_val / trigger_val

            # Final Logic: (MXMN_low < 10 AND Ratio1 < 2) OR (MXMN_low < 10 AND Ratio2 < 2)
            return ratio1 < 2 or ratio2 < 2

        df_mv2["is_moment"] = df_mv2.apply(check_moment, axis=1)
        
        # Other Filter Logic
        df_mv2["is_conso"] = df_mv2[c_mxmn].apply(lambda x: safe_float(x) != -1.0 and safe_float(x) < 15)
        df_mv2["is_jump"] = df_mv2[c_clabove].apply(lambda x: safe_float(x) > 3)

        # 4. Selenium Setup
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--window-size=1920,1080")
        
        driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
        
        # TradingView Cookie Injection
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if cookie_data:
            driver.get("https://www.tradingview.com/")
            time.sleep(2)
            for c in json.loads(cookie_data):
                try:
                    driver.add_cookie({
                        "name": c["name"], 
                        "value": c["value"], 
                        "domain": ".tradingview.com",
                        "path": "/"
                    })
                except: continue
            driver.refresh()
            log("✅ Cookies injected.")

        # 5. Execute Scans and Screenshots
        filters = [
            ("is_moment", "Moment Filter"),
            ("is_conso", "Conso Filter"),
            ("is_jump", "Jump Filter")
        ]

        for bool_col, filter_label in filters:
            targets = df_mv2[df_mv2[bool_col] == True]
            log(f"🔍 Scanning {filter_label}: {len(targets)} matches found.")
            
            for _, row in targets.iterrows():
                symbol = safe_str(row.iloc[0])
                for tf, url_map in [("day", day_urls), ("week", week_urls)]:
                    url = url_map.get(symbol)
                    if not url or "tradingview.com" not in url:
                        continue
                    
                    try:
                        driver.get(url)
                        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                        )
                        time.sleep(POST_LOAD_SLEEP)
                        img_data = chart.screenshot_as_png
                        
                        # Save to Database
                        cur = db_conn.cursor()
                        sql = f"""
                            INSERT INTO `{TARGET_TABLE}` 
                            (symbol, timeframe, filter_type, day, screenshot) 
                            VALUES (%s, %s, %s, 0, %s) 
                            ON DUPLICATE KEY UPDATE screenshot=VALUES(screenshot), day=0
                        """
                        cur.execute(sql, (symbol, tf, filter_label, img_data))
                        cur.close()
                        log(f"✅ Saved {symbol} | {tf} | {filter_label}")
                        
                    except Exception as e:
                        log(f"❌ Screenshot Error {symbol} ({tf}): {e}")

        log("🏁 All processing finished successfully.")

    except Exception as e:
        log(f"❌ Fatal Error: {e}")

    finally:
        if driver:
            driver.quit()
        if db_conn:
            db_conn.close()

if __name__ == "__main__":
    main()
