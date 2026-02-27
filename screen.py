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
RETRY_PER_SYMBOL = 2

# ✅ speed: install driver path once
CHROME_DRIVER_PATH = ChromeDriverManager().install()


def log(msg):
    print(msg, flush=True)


def safe_float(v, default=0.0):
    try:
        if v is None:
            return default
        s = str(v).replace("%", "").strip()
        if s == "" or s.lower() == "none":
            return default
        return float(s)
    except:
        return default


def open_db():
    conn = mysql.connector.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def clear_db_before_run(conn):
    cur = None
    try:
        cur = conn.cursor()
        log("🧹 Clearing old database entries...")
        cur.execute("TRUNCATE TABLE stock_screenshots")
        log("✅ Database is clean.")
    except Exception as e:
        log(f"❌ Error clearing database: {e}")
    finally:
        try:
            if cur:
                cur.close()
        except:
            pass


def save_to_mysql(conn, symbol, timeframe, image_data):
    """
    Inserts / updates row based on UNIQUE(symbol,timeframe).
    """
    cur = None
    try:
        cur = conn.cursor()
        q = """
            INSERT INTO stock_screenshots (symbol, timeframe, screenshot)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                screenshot = VALUES(screenshot),
                created_at = CURRENT_TIMESTAMP
        """
        cur.execute(q, (symbol, timeframe, image_data))
        log(f"✅ [DB] Updated/Saved {symbol} ({timeframe})")
        return True
    except Exception as e:
        log(f"❌ Database Error ({symbol} {timeframe}): {e}")
        return False
    finally:
        try:
            if cur:
                cur.close()
        except:
            pass


def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )

    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def inject_tv_cookies(driver):
    """
    Inject TradingView cookies (from env TRADINGVIEW_COOKIES JSON).
    """
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("❌ TRADINGVIEW_COOKIES missing.")
            return False

        cookies = json.loads(cookie_data)

        driver.get("https://www.tradingview.com/")
        time.sleep(3)

        ok = 0
        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"),
                    "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"),
                    "path": c.get("path", "/"),
                })
                ok += 1
            except:
                pass

        driver.refresh()
        time.sleep(4)
        log(f"✅ Cookies injected: {ok}/{len(cookies)}")
        return ok > 0
    except Exception as e:
        log(f"❌ Cookie inject error: {e}")
        return False


def wait_chart(driver):
    return WebDriverWait(driver, CHART_WAIT_SEC).until(
        EC.visibility_of_element_located(
            (By.XPATH, "//div[contains(@class, 'chart-container')]")
        )
    )


def open_url_with_retry(driver, url, retries=2):
    for attempt in range(1, retries + 1):
        try:
            driver.get(url)
            return True
        except Exception as e:
            log(f"⚠️ Page load failed (attempt {attempt}/{retries}): {e}")
            time.sleep(3)
    return False


def set_timeframe(driver, tf_key):
    try:
        ActionChains(driver).send_keys(tf_key).send_keys(Keys.ENTER).perform()
        time.sleep(3)
        return True
    except Exception as e:
        log(f"⚠️ Timeframe set failed ({tf_key}): {e}")
        return False


def main():
    log(f"🔎 DB TARGET host={DB_CONFIG.get('host')} db={DB_CONFIG.get('database')} user={DB_CONFIG.get('user')}")

    # DB connect once
    try:
        conn = open_db()
    except Exception as e:
        log(f"❌ DB connection failed: {e}")
        return

    # keep your behavior: clear each run
    clear_db_before_run(conn)

    # Sheets
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json:
            log("❌ GSPREAD_CREDENTIALS missing.")
            return

        client = gspread.service_account_from_dict(json.loads(creds_json))

        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        if not mv2_raw or len(mv2_raw) < 2:
            log("❌ MV2 sheet empty or has no rows.")
            return
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        if not stock_raw or len(stock_raw) < 2:
            log("❌ Stock List sheet empty or has no rows.")
            return
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        # same mapping rule: col A = symbol, col C = URL
        if df_stocks.shape[1] < 3:
            log("❌ Stock List sheet must have at least 3 columns (Symbol in col A, URL in col C).")
            return

        link_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))

        log(f"✅ Loaded MV2 rows: {len(df_mv2)} | Stock links: {len(link_map)}")

    except Exception as e:
        log(f"❌ Sheet Error: {e}")
        return

    # Selenium
    driver = get_driver()
    try:
        if not inject_tv_cookies(driver):
            log("❌ Cookie injection failed, stopping.")
            return

        qualified_symbols = 0
        saved_rows = 0

        for _, row in df_mv2.iterrows():
            symbol = str(row.get("Symbol", "")).strip()
            if not symbol:
                continue

            # sector rejection (same)
            sector = str(row.get("Sector", "")).strip().upper()
            if sector in ("INDICES", "MUTUAL FUND SCHEME"):
                continue

            daily = safe_float(row.get("dailychange", 0))
            monthly = safe_float(row.get("monthlychange", 0))

            # same threshold logic
            if not (daily >= DAILY_THRESHOLD or monthly >= MONTHLY_THRESHOLD):
                continue

            qualified_symbols += 1

            url = link_map.get(symbol)
            if (not url) or ("tradingview.com" not in url):
                log(f"⚠️ Missing/invalid TV link for {symbol}.")
                continue

            if not open_url_with_retry(driver, url, retries=RETRY_PER_SYMBOL):
                log(f"⚠️ Could not open url for {symbol}.")
                continue

            ok_symbol = False
            for attempt in range(1, RETRY_PER_SYMBOL + 1):
                try:
                    chart = wait_chart(driver)
                    time.sleep(POST_LOAD_SLEEP)

                    if daily >= DAILY_THRESHOLD:
                        set_timeframe(driver, "1D")
                        if save_to_mysql(conn, symbol, "daily", chart.screenshot_as_png):
                            saved_rows += 1

                    if monthly >= MONTHLY_THRESHOLD:
                        set_timeframe(driver, "1M")
                        if save_to_mysql(conn, symbol, "monthly", chart.screenshot_as_png):
                            saved_rows += 1

                    ok_symbol = True
                    break

                except Exception as e:
                    log(f"⚠️ Screenshot Error ({symbol}) attempt {attempt}/{RETRY_PER_SYMBOL}: {e}")
                    time.sleep(3)

            if not ok_symbol:
                log(f"❌ Failed completely for {symbol} after retries.")

        log(f"✅ QUALIFIED SYMBOLS: {qualified_symbols}")
        log(f"✅ SAVED ROWS (daily+monthly): {saved_rows}")
        log("🏁 DONE!")

    finally:
        try:
            driver.quit()
        except:
            pass
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
