import os, time, json, re
import gspread
import pandas as pd
import mysql.connector

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

# Thresholds in PERCENT units (because columns are change% and mchange%)
DAILY_THRESHOLD_PCT   = float(os.getenv("DAILY_THRESHOLD_PCT", "0.07"))   # 0.07% default
MONTHLY_THRESHOLD_PCT = float(os.getenv("MONTHLY_THRESHOLD_PCT", "0.25")) # 0.25% default


# ---------------- HELPERS ---------------- #
def parse_percent_any(val) -> float:
    """
    Converts values like:
      "7%" -> 7.0
      "0.07%" -> 0.07
      "7" -> 7.0
      7 -> 7.0
      ""/None -> 0.0
    Returns percent units (NOT fraction).
    """
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "-"):
        return 0.0
    s = s.replace(",", "")
    s = re.sub(r"\s+", "", s)
    s = s.replace("%", "")
    # keep only number-ish (handles stray chars)
    m = re.search(r"-?\d+(\.\d+)?", s)
    if not m:
        return 0.0
    try:
        return float(m.group(0))
    except:
        return 0.0


def clear_db_before_run():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("🧹 Clearing old database entries...", flush=True)
        cursor.execute("TRUNCATE TABLE stock_screenshots")
        conn.commit()
        print("✅ Database is clean.", flush=True)
    except Exception as e:
        print(f"❌ Error clearing database: {e}", flush=True)
    finally:
        try:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals() and conn.is_connected(): conn.close()
        except:
            pass


def save_to_mysql(symbol, timeframe, image_data):
    try:
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
        print(f"✅ [DB] Updated/Saved {symbol} ({timeframe})", flush=True)
    except Exception as e:
        print(f"❌ Database Error: {e}", flush=True)
    finally:
        try:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals() and conn.is_connected(): conn.close()
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
    return driver


def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            print("❌ TRADINGVIEW_COOKIES env missing.", flush=True)
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
        time.sleep(5)
        print("✅ TradingView cookies injected.", flush=True)
        return True
    except Exception as e:
        print(f"❌ Cookie inject error: {e}", flush=True)
        return False


def send_timeframe_keys(driver, keys_text: str):
    # More reliable than webdriver.ActionChains reference
    ActionChains(driver).send_keys(keys_text).send_keys(Keys.ENTER).perform()


# ---------------- MAIN ---------------- #
def main():
    clear_db_before_run()

    # --- Load sheets ---
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json:
            print("❌ GSPREAD_CREDENTIALS env missing.", flush=True)
            return

        client = gspread.service_account_from_dict(json.loads(creds_json))

        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        if not mv2_raw or len(mv2_raw) < 2:
            print("❌ MV2 sheet empty.", flush=True)
            return
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        if not stock_raw or len(stock_raw) < 2:
            print("❌ Stock list sheet empty.", flush=True)
            return
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        # Map: column A => Symbol, column C => TradingView URL
        link_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))

        # quick sanity
        print(f"📄 MV2 rows: {len(df_mv2)} | StockList rows: {len(df_stocks)}", flush=True)

    except Exception as e:
        print(f"❌ Sheet Error: {e}", flush=True)
        return

    # --- Browser ---
    driver = get_driver()
    if not inject_tv_cookies(driver):
        try: driver.quit()
        except: pass
        return

    processed = 0
    saved = 0

    for _, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        if not symbol:
            continue

        # ✅ SECTOR REJECTION (kept)
        sector = str(row.get('Sector', '')).strip().upper()
        if sector in ("INDICES", "MUTUAL FUND SCHEME"):
            continue

        # ✅ Use your NEW columns: change% and mchange%
        daily_pct   = parse_percent_any(row.get('change%', '0'))
        monthly_pct = parse_percent_any(row.get('mchange%', '0'))

        # Filter by thresholds (in percent units)
        if daily_pct < DAILY_THRESHOLD_PCT and monthly_pct < MONTHLY_THRESHOLD_PCT:
            continue

        url = link_map.get(symbol)
        if not url or "tradingview.com" not in url:
            continue

        processed += 1
        print(f"➡️ {symbol} | change%={daily_pct} | mchange%={monthly_pct}", flush=True)

        try:
            driver.get(url)

            chart = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//div[contains(@class, 'chart-container')]")
                )
            )

            # give chart time to fully paint
            time.sleep(6)

            if daily_pct >= DAILY_THRESHOLD_PCT:
                send_timeframe_keys(driver, "1D")
                time.sleep(4)
                save_to_mysql(symbol, "daily", chart.screenshot_as_png)
                saved += 1

            if monthly_pct >= MONTHLY_THRESHOLD_PCT:
                send_timeframe_keys(driver, "1M")
                time.sleep(4)
                save_to_mysql(symbol, "monthly", chart.screenshot_as_png)
                saved += 1

        except Exception as e:
            print(f"⚠️ Screenshot Error ({symbol}): {e}", flush=True)

    try: driver.quit()
    except: pass

    print(f"🏁 DONE! processed={processed} saved={saved}", flush=True)


if __name__ == "__main__":
    main()
