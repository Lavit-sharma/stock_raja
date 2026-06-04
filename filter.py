import os
import time
import json
import gspread
import pandas as pd
import mysql.connector
from mysql.connector import pooling

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
POST_LOAD_SLEEP = 5
DB_RETRY = 3
MAX_DAY_TO_KEEP = 4

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_int(v):
    try:
        if v is None or str(v).strip() == "" or str(v).lower() == "nan":
            return -1
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return -1

def safe_float(v):
    try:
        if v is None or str(v).strip() == "" or str(v).lower() == "nan":
            return 0.0
        return float(str(v).strip()))
    except (ValueError, TypeError):
        return 0.0

def fix_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [
            f"{dup}_{i}" if i != 0 else dup
            for i in range(sum(cols == dup))
        ]
    df.columns = cols
    return df

# ---------------- DB CLASS ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        # Use a connection pool for robust connection lifecycle management
        self.pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=3,
            pool_reset_session=True,
            **self.config
        )

    def _execute_with_retry(self, query, params=None, is_update=False):
        """Executes a query with built-in connection checking and retry logic."""
        last_exception = None
        for attempt in range(DB_RETRY):
            conn = None
            try:
                conn = self.pool.get_connection()
                conn.autocommit = True
                
                # Double check the connection health before processing
                if not conn.is_connected():
                    conn.reconnect(attempts=3, delay=1)
                
                with conn.cursor() as cur:
                    if params:
                        cur.execute(query, params)
                    else:
                        cur.execute(query)
                    
                    if not is_update:
                        return cur.fetchall()
                    return True
                    
            except (mysql.connector.Error, Exception) as e:
                last_exception = e
                log(f"⚠️ SQL Execution failure (Attempt {attempt + 1}/{DB_RETRY}): {e}")
                time.sleep(1.5)
            finally:
                if conn and conn.is_connected():
                    conn.close()  # Returns connection back to the pool cleanly
                    
        raise Exception(f"❌ Failed to execute SQL query after {DB_RETRY} attempts. Error: {last_exception}")

    def execute_update(self, query, params=None):
        return self._execute_with_retry(query, params, is_update=True)

    def close(self):
        # Pool handles closing resources inherently, placeholder for interface compatibility
        pass

# ---------------- CORE LOGIC ---------------- #
def roll_days_forward(db: DB):
    try:
        db.execute_update(f"UPDATE `{TARGET_TABLE}` SET `day` = `day` + 1")
        
        db.execute_update(
            f"""
            DELETE FROM `{TARGET_TABLE}`
            WHERE `day` > %s
            AND LOWER(TRIM(COALESCE(`review_status`, ''))) = 'rejected'
            """,
            (MAX_DAY_TO_KEEP,)
        )
        log("✅ Rollover successful.")
    except Exception as e:
        log(f"❌ Critical error during rollover sequence: {e}")

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        roll_days_forward(db)

        # ---------------- LOAD DATA ---------------- #
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        mv2_sheet = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_sheet[1:], columns=[c.strip() for c in mv2_sheet[0]])
        df_mv2 = fix_duplicate_columns(df_mv2)

        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID).get_all_values()

        url_map = {
            row[0].strip(): {
                "week": row[2].strip(),
                "day": row[3].strip()
            }
            for row in stock_ws[1:]
            if row and row[0].strip()
        }

        # ---------------- FILTER PROCESSING ---------------- #
        cols_to_fix = ["D_Trigger", "D_Trigger_S"]
        for col in cols_to_fix:
            if col in df_mv2.columns:
                df_mv2[f"{col}_n"] = df_mv2[col].apply(safe_int)
            else:
                df_mv2[f"{col}_n"] = -1

        # ---------------- COMPACT FILTER ---------------- #
        compact_filter_df = df_mv2[
            (df_mv2["MXMN_low"].apply(safe_float) == 1)
            & (df_mv2["D_CL_AB"].apply(safe_float) > 1)
            & (df_mv2["D_CL_AB"].apply(safe_float) < 1.03)
            & (df_mv2["MXMN"].apply(safe_float) < 30)
        ]

        # ---------------- TRIGGERS ---------------- #
        triggers = {
            "D_Trigger": df_mv2[df_mv2["D_Trigger_n"] == 0],
            "D_Trigger_S": df_mv2[(df_mv2["D_Trigger_S_n"] == 0) & (df_mv2["D_Trigger_S_n"] != df_mv2["D_Trigger_n"])],
            "Compact_Filter": compact_filter_df
        }

        # ---------------- DEBUG LOGS ---------------- #
        for name, d_sub in triggers.items():
            symbols_found = d_sub.iloc[:, 0].astype(str).tolist()
            log(
                f"🔍 Filter Check: {name} | "
                f"Found: {len(d_sub)} | "
                f"Symbols: {', '.join(symbols_found) if symbols_found else 'None'}"
            )

        # ---------------- SETUP BROWSER ---------------- #
        driver = get_driver()
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")

        if cookie_data:
            driver.get("https://www.tradingview.com/")
            for c in json.loads(cookie_data):
                try:
                    driver.add_cookie({
                        "name": c["name"],
                        "value": c["value"],
                        "domain": ".tradingview.com",
                        "path": "/"
                    })
                except:
                    continue
            driver.refresh()

        # ---------------- EXECUTE SCREENSHOTS ---------------- #
        for filter_name, matched_df in triggers.items():
            if matched_df.empty:
                continue

            log(f"🚀 Processing {filter_name}...")

            for _, row in matched_df.iterrows():
                symbol = str(row.iloc[0]).strip()
                urls = url_map.get(symbol)

                if not urls:
                    continue

                for tf in ["day", "week"]:
                    url = urls.get(tf)
                    if not url or "tradingview.com" not in url:
                        continue

                    try:
                        driver.get(url)
                        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                        )

                        log(f"    ⏳ Waiting {POST_LOAD_SLEEP}s for late popups: {symbol} ({tf})...")
                        time.sleep(POST_LOAD_SLEEP)

                        # ---------------- REMOVE POPUPS ---------------- #
                        was_removed = driver.execute_script("""
                            var found = false;
                            var popups = document.querySelectorAll(
                                '[class*="overlap-manager-root"], \
                                [class*="modal-"], \
                                [class*="dialog-"], \
                                [class*="backdrops-"]'
                            );
                            if (popups.length > 0) {
                                popups.forEach(function(p) { p.remove(); });
                                found = true;
                            }
                            document.body.style.overflow = 'auto';
                            document.body.style.position = 'static';
                            var chartElem = document.querySelector('.chart-container-border');
                            if(chartElem) { chartElem.click(); }
                            return found;
                        """)

                        if was_removed:
                            log(f"    🧹 Popup detected and removed for {symbol} ({tf})")
                        else:
                            log(f"    ✨ No popups found for {symbol} ({tf})")

                        time.sleep(1)
                        img = chart.screenshot_as_png

                        # ---------------- DATA INSERTION ---------------- #
                        insert_query = f"""
                            INSERT INTO `{TARGET_TABLE}`
                            (symbol, timeframe, filter_type, day, screenshot)
                            VALUES (%s, %s, %s, 0, %s)
                        """
                        insert_params = (symbol, tf, filter_name, img)
                        
                        db.execute_update(insert_query, insert_params)
                        log(f"    ✅ Saved {symbol} ({tf})")

                    except Exception as e:
                        log(f"    ❌ Error {symbol} {tf}: {e}")

        log("🏁 Execution Finished.")

    except Exception as e:
        log(f"❌ Fatal: {e}")
    finally:
        if driver:
            driver.quit()
        db.close()

if __name__ == "__main__":
    main()
