import os, time, json, gspread
import pandas as pd
import mysql.connector
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# ---------------- LOGGING ---------------- #
def log(msg):
    print(msg, flush=True)

def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

# ---------------- HELPERS ---------------- #

def clear_db_before_run():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        log(f"[{now_ts()}] 🧹 Clearing old database entries...")
        cursor.execute("TRUNCATE TABLE stock_screenshots")
        conn.commit()
        log(f"[{now_ts()}] ✅ Database is clean.")
    except Exception as e:
        log(f"[{now_ts()}] ❌ Error clearing database: {e}")
    finally:
        try:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
        except:
            pass

def save_to_mysql(symbol, timeframe, image_data):
    try:
        if not image_data or len(image_data) < 5000:
            # TradingView blank/empty screenshots are often very small
            log(f"[{now_ts()}] ⚠️ [{symbol}] {timeframe} screenshot looks too small ({0 if not image_data else len(image_data)} bytes) — skipping DB save.")
            return False

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            INSERT INTO stock_screenshots (symbol, timeframe, screenshot)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                screenshot = VALUES(screenshot),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data))
        conn.commit()
        log(f"[{now_ts()}] ✅ [DB] Updated/Saved {symbol} ({timeframe}) | bytes={len(image_data)}")
        return True
    except Exception as e:
        log(f"[{now_ts()}] ❌ Database Error ({symbol}, {timeframe}): {e}")
        return False
    finally:
        try:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
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
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_page_load_timeout(60)
    return driver

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log(f"[{now_ts()}] ❌ TRADINGVIEW_COOKIES env not set.")
            return False

        cookies = json.loads(cookie_data)
        log(f"[{now_ts()}] 🍪 Injecting TradingView cookies...")

        driver.get("https://www.tradingview.com/")
        time.sleep(3)

        injected = 0
        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"),
                    "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"),
                    "path": c.get("path", "/")
                })
                injected += 1
            except:
                pass

        driver.refresh()
        time.sleep(5)

        log(f"[{now_ts()}] ✅ Cookies injected: {injected}/{len(cookies)}")
        return True
    except Exception as e:
        log(f"[{now_ts()}] ❌ Cookie inject failed: {e}")
        return False

def parse_percent(val):
    """
    Accepts '7%', '7', '0.07', 0.07 etc.
    Returns float as a fraction (0.07 for 7%).
    """
    s = str(val if val is not None else "0").strip().replace("%", "")
    if s == "":
        return 0.0
    try:
        x = float(s)
        # If the sheet gives 7 instead of 0.07, normalize
        if x > 1.0:
            x = x / 100.0
        return x
    except:
        return 0.0

def safe_set_timeframe(driver, key_text, symbol):
    """
    Sends timeframe keys reliably.
    """
    try:
        ActionChains(driver).send_keys(key_text).send_keys(Keys.ENTER).perform()
        time.sleep(2)
        return True
    except Exception as e:
        log(f"[{now_ts()}] ⚠️ [{symbol}] Failed to set timeframe {key_text}: {e}")
        return False

def get_chart_element(driver, symbol):
    """
    Keeps your same XPATH, but adds better waits + scroll.
    """
    chart = WebDriverWait(driver, 30).until(
        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
    )
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", chart)
    except:
        pass

    # Give TV time to render canvas/webgl
    time.sleep(6)
    return chart

def screenshot_chart_or_page(driver, chart, symbol, timeframe):
    """
    Prefer chart element screenshot; if it looks tiny, fallback to full-page screenshot.
    """
    img = b""
    try:
        img = chart.screenshot_as_png
    except Exception as e:
        log(f"[{now_ts()}] ⚠️ [{symbol}] element screenshot failed ({timeframe}): {e}")

    if not img or len(img) < 5000:
        # fallback to full page screenshot (often fixes "blank element" issue)
        try:
            img2 = driver.get_screenshot_as_png()
            log(f"[{now_ts()}] 🔁 [{symbol}] Fallback full-page screenshot ({timeframe}) bytes={len(img2)}")
            return img2
        except Exception as e:
            log(f"[{now_ts()}] ❌ [{symbol}] full-page screenshot failed ({timeframe}): {e}")
            return b""

    return img

# ---------------- MAIN ---------------- #

def main():
    clear_db_before_run()

    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json:
            log(f"[{now_ts()}] ❌ GSPREAD_CREDENTIALS env not set.")
            return

        client = gspread.service_account_from_dict(json.loads(creds_json))

        log(f"[{now_ts()}] 📥 Loading MV2 sheet...")
        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])
        log(f"[{now_ts()}] ✅ MV2 rows loaded: {len(df_mv2)}")

        log(f"[{now_ts()}] 📥 Loading STOCK LIST sheet...")
        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        log(f"[{now_ts()}] ✅ Stock list rows loaded: {len(df_stocks)}")

        # map: column A (symbol) -> column C (tradingview url)
        link_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))

        log(f"[{now_ts()}] 🔗 Link map created: {len(link_map)} symbols")
    except Exception as e:
        log(f"[{now_ts()}] ❌ Sheet Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    processed = 0
    matched = 0
    saved = 0
    skipped_sector = 0
    skipped_no_url = 0

    for idx, row in df_mv2.iterrows():
        processed += 1
        symbol = str(row.get('Symbol', '')).strip()
        if not symbol:
            continue

        sector = str(row.get('Sector', '')).strip().upper()
        if sector in ("INDICES", "MUTUAL FUND SCHEME"):
            skipped_sector += 1
            continue

        daily = parse_percent(row.get('change%', '0'))
        monthly = parse_percent(row.get('mchange%', '0'))

        # log minimal per row
        # log(f"[{now_ts()}] [{processed}/{len(df_mv2)}] {symbol} daily={daily:.4f} monthly={monthly:.4f}")

        if daily >= 0.07 or monthly >= 0.25:
            matched += 1
            url = link_map.get(symbol)
            if not url or "tradingview.com" not in url:
                skipped_no_url += 1
                log(f"[{now_ts()}] ⚠️ [{symbol}] No valid TradingView URL in stock list.")
                continue

            log(f"[{now_ts()}] 🌐 [{matched}] Opening {symbol} | daily={daily:.2%} monthly={monthly:.2%}")
            try:
                driver.get(url)
            except Exception as e:
                log(f"[{now_ts()}] ❌ [{symbol}] Page load failed: {e}")
                continue

            try:
                chart = get_chart_element(driver, symbol)

                if daily >= 0.07:
                    safe_set_timeframe(driver, "1D", symbol)
                    time.sleep(3)
                    img = screenshot_chart_or_page(driver, chart, symbol, "daily")
                    if save_to_mysql(symbol, "daily", img):
                        saved += 1

                if monthly >= 0.25:
                    safe_set_timeframe(driver, "1M", symbol)
                    time.sleep(3)
                    img = screenshot_chart_or_page(driver, chart, symbol, "monthly")
                    if save_to_mysql(symbol, "monthly", img):
                        saved += 1

            except Exception as e:
                log(f"[{now_ts()}] ⚠️ Screenshot Error ({symbol}): {e}")

    driver.quit()
    log(f"[{now_ts()}] 🏁 DONE! processed={processed} matched={matched} saved={saved} skipped_sector={skipped_sector} skipped_no_url={skipped_no_url}")

if __name__ == "__main__":
    main()
