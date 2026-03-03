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
PAGE_RETRY = 2

CHROME_DRIVER_PATH = ChromeDriverManager().install()


# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)


def safe_str(v):
    try:
        return str(v).strip()
    except:
        return ""


def safe_int(v):
    try:
        return int(float(str(v).strip()))
    except:
        return 0


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


def save_to_mysql(db: DB, symbol, timeframe, image, mv2_n_al_json):
    query = f"""
        INSERT INTO `{TARGET_TABLE}`
            (symbol, timeframe, screenshot, mv2_n_al)
        VALUES
            (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            mv2_n_al = VALUES(mv2_n_al),
            created_at = CURRENT_TIMESTAMP
    """

    for attempt in range(DB_RETRY):
        try:
            conn = db.ensure()
            cur = conn.cursor()
            cur.execute(query, (symbol, timeframe, image, mv2_n_al_json))
            cur.close()
            log(f"✅ Saved {symbol} ({timeframe})")
            return
        except Exception as e:
            log(f"⚠️ DB error attempt {attempt+1}: {e}")
            db.connect()
            time.sleep(1.5)

    log(f"❌ Failed saving {symbol} ({timeframe})")


# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)

    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    driver.set_page_load_timeout(60)
    return driver


def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("❌ TRADINGVIEW_COOKIES missing.")
            return False

        cookies = json.loads(cookie_data)

        driver.get("https://www.tradingview.com/")
        time.sleep(3)

        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"),
                    "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"),
                    "path": c.get("path", "/")
                })
            except:
                pass

        driver.refresh()
        time.sleep(4)
        log("✅ Cookies injected")
        return True
    except Exception as e:
        log(f"❌ Cookie error: {e}")
        return False


def wait_chart(driver):
    return WebDriverWait(driver, CHART_WAIT_SEC).until(
        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
    )


def open_with_retry(driver, url):
    for i in range(PAGE_RETRY):
        try:
            driver.get(url)
            return True
        except Exception as e:
            log(f"⚠️ Page load retry {i+1}: {e}")
            time.sleep(2)
    return False


# ---------------- MAIN ---------------- #
def main():
    log(f"🎯 Saving into table: {TARGET_TABLE}")

    db = DB(DB_CONFIG)

    try:
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        # MV2 SQL
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        # Stock List
        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID)
        stock_raw = stock_ws.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        week_url_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(),
                                df_stocks.iloc[:, 2].astype(str).str.strip()))

        day_url_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(),
                               df_stocks.iloc[:, 3].astype(str).str.strip()))

    except Exception as e:
        log(f"❌ Sheet error: {e}")
        return

    if "D_Trigger" not in df_mv2.columns:
        log("❌ Column D_Trigger not found.")
        return

    driver = get_driver()

    try:
        if not inject_tv_cookies(driver):
            return

        headers = list(df_mv2.columns)

        for _, row in df_mv2.iterrows():
            symbol = safe_str(row.iloc[0])
            if not symbol:
                continue

            d_trigger = safe_int(row["D_Trigger"])

            if d_trigger != 1:
                continue  # Only process when D_Trigger == 1

            n_al_map = {
                safe_str(headers[i]): safe_str(row.iloc[i])
                for i in range(13, min(37, len(headers)))
            }
            mv2_n_al_json = json.dumps(n_al_map, ensure_ascii=False)

            day_url = day_url_map.get(symbol)
            week_url = week_url_map.get(symbol)

            # DAY
            if day_url and "tradingview.com" in day_url:
                if open_with_retry(driver, day_url):
                    chart = wait_chart(driver)
                    time.sleep(POST_LOAD_SLEEP)
                    save_to_mysql(db, symbol, "day", chart.screenshot_as_png, mv2_n_al_json)

            # WEEK
            if week_url and "tradingview.com" in week_url:
                if open_with_retry(driver, week_url):
                    chart = wait_chart(driver)
                    time.sleep(POST_LOAD_SLEEP)
                    save_to_mysql(db, symbol, "week", chart.screenshot_as_png, mv2_n_al_json)

        log("🏁 DONE")

    finally:
        try:
            driver.quit()
        except:
            pass
        db.close()


if __name__ == "__main__":
    main()
