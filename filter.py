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

# Keep day 0 to day 4 = total 5 days
MAX_DAY_TO_KEEP = 4


# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def safe_str(v):
    return str(v).strip() if v else ""

def safe_int(v):
    try:
        val = str(v).strip()
        if not val:
            return -1
        return int(float(val))
    except (ValueError, TypeError):
        return -1


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
    update_query = f"UPDATE `{TARGET_TABLE}` SET `day` = `day` + 1"
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
            log(f"⚠️ Rollover error: {e}")
            db.connect()
            time.sleep(1)


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
            log(f"⚠️ DB error: {e}")
            db.connect()
            time.sleep(1)


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
        return True
    except Exception:
        return False


# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        roll_days_forward(db)

        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        week_urls = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))
        day_urls = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 3].astype(str).str.strip()
        ))

        driver = get_driver()
        if not inject_tv_cookies(driver):
            log("❌ Cookie injection failed.")
            return

        # --- UPDATED CONDITION LOGIC ---
        for _, row in df_mv2.iterrows():
            symbol = safe_str(row.iloc[0])
            if not symbol:
                continue

            d_val = safe_int(row.get("D_Trigger"))
            s_val = safe_int(row.get("D_Trigger_S"))

            active_trigger = None

            # Condition 1: D_Trigger is 0
            if d_val == 0:
                active_trigger = "D_Trigger"
            # Condition 2: D_Trigger_S is 0 AND D_Trigger is NOT 0
            elif s_val == 0:
                active_trigger = "D_Trigger_S"

            if active_trigger:
                log(f"🚀 Triggered: {symbol} ({active_trigger}=0)")
                tasks = [("day", day_urls.get(symbol)), ("week", week_urls.get(symbol))]

                for tf_name, url in tasks:
                    if url and "tradingview.com" in url:
                        try:
                            driver.get(url)
                            chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                            )
                            time.sleep(POST_LOAD_SLEEP)
                            save_screenshot(db, symbol, tf_name, active_trigger, chart.screenshot_as_png)
                        except Exception as e:
                            log(f"❌ Screenshot failed for {symbol} {tf_name}: {e}")

        log("🏁 All triggers processed.")

    finally:
        if driver:
            driver.quit()
        db.close()


if __name__ == "__main__":
    main()
