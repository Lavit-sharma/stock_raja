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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from webdriver_manager.chrome import ChromeDriverManager


# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

DAILY_THRESHOLD   = 0.07
MONTHLY_THRESHOLD = 0.25

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6

CHROME_DRIVER_PATH = ChromeDriverManager().install()


# ---------------- HELPERS ---------------- #

def log(msg):
    print(msg, flush=True)


def safe_float(v):
    try:
        return float(str(v).replace('%', '').strip())
    except:
        return 0.0


def open_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def clear_db_before_run(conn):
    cur = conn.cursor()
    log("🧹 Clearing old database entries...")
    cur.execute("TRUNCATE TABLE stock_screenshots")
    log("✅ Database is clean.")
    cur.close()


def save_to_mysql(conn, symbol, timeframe, image):
    cur = conn.cursor()

    query = """
        INSERT INTO stock_screenshots (symbol, timeframe, screenshot)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            screenshot = VALUES(screenshot),
            created_at = CURRENT_TIMESTAMP
    """

    cur.execute(query, (symbol, timeframe, image))
    log(f"✅ [DB] Updated/Saved {symbol} ({timeframe})")

    cur.close()


# ---------------- SELENIUM ---------------- #

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)

    driver.execute_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
    )

    return driver


def inject_tv_cookies(driver):
    try:
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))
        driver.get("https://www.tradingview.com/")
        time.sleep(3)

        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c["name"],
                    "value": c["value"],
                    "domain": ".tradingview.com",
                    "path": "/"
                })
            except:
                pass

        driver.refresh()
        time.sleep(4)
        log("✅ Cookies injected")
        return True
    except Exception as e:
        log(f"❌ Cookie error {e}")
        return False


def wait_chart(driver):
    return WebDriverWait(driver, CHART_WAIT_SEC).until(
        EC.visibility_of_element_located(
            (By.XPATH, "//div[contains(@class,'chart-container')]")
        )
    )


def set_tf(driver, tf):
    ActionChains(driver).send_keys(tf).send_keys(Keys.ENTER).perform()
    time.sleep(3)


# ---------------- MAIN ---------------- #

def main():

    log(f"🔎 DB TARGET {DB_CONFIG['host']} / {DB_CONFIG['database']}")

    conn = open_db()
    clear_db_before_run(conn)

    # ---- Sheets ----
    client = gspread.service_account_from_dict(
        json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    )

    mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
    df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

    stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
    df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

    link_map = dict(zip(
        df_stocks.iloc[:,0].astype(str).str.strip(),
        df_stocks.iloc[:,2].astype(str).str.strip()
    ))

    log(f"✅ Loaded MV2 rows: {len(df_mv2)}")

    # ---- Browser ----
    driver = get_driver()

    if not inject_tv_cookies(driver):
        return

    qualified = 0
    saved = 0

    for _, row in df_mv2.iterrows():

        symbol = str(row.iloc[0]).strip()   # Column A = Symbol
        sector = str(row.iloc[1]).upper()   # Column B = Sector

        # ❌ skip unwanted sectors
        if sector in ("INDICES", "MUTUAL FUND SCHEME"):
            continue

        # ✅ DIRECT COLUMN ACCESS
        daily   = safe_float(row.iloc[14])  # Column O
        monthly = safe_float(row.iloc[15])  # Column P

        if not (daily >= DAILY_THRESHOLD or monthly >= MONTHLY_THRESHOLD):
            continue

        qualified += 1

        url = link_map.get(symbol)
        if not url:
            continue

        driver.get(url)

        try:
            chart = wait_chart(driver)
            time.sleep(POST_LOAD_SLEEP)

            if daily >= DAILY_THRESHOLD:
                set_tf(driver, "1D")
                save_to_mysql(conn, symbol, "daily", chart.screenshot_as_png)
                saved += 1

            if monthly >= MONTHLY_THRESHOLD:
                set_tf(driver, "1M")
                save_to_mysql(conn, symbol, "monthly", chart.screenshot_as_png)
                saved += 1

        except Exception as e:
            log(f"⚠️ Screenshot error {symbol}: {e}")

    driver.quit()
    conn.close()

    log(f"✅ QUALIFIED SYMBOLS: {qualified}")
    log(f"✅ SAVED ROWS: {saved}")
    log("🏁 DONE!")


if __name__ == "__main__":
    main()
