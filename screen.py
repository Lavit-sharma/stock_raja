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


# ===================== CONFIG ===================== #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# ✅ Rules (INTENDED): 7% daily, 25% monthly like your original logic
DAILY_THRESHOLD_PCT   = float(os.getenv("DAILY_THRESHOLD_PCT", "7"))    # 7%
MONTHLY_THRESHOLD_PCT = float(os.getenv("MONTHLY_THRESHOLD_PCT", "25")) # 25%

# Selenium timing
PAGE_LOAD_SLEEP = float(os.getenv("PAGE_LOAD_SLEEP", "2.0"))
CHART_PAINT_SLEEP = float(os.getenv("CHART_PAINT_SLEEP", "4.0"))
TF_SLEEP = float(os.getenv("TF_SLEEP", "3.0"))
WAIT_TIMEOUT = int(os.getenv("WAIT_TIMEOUT", "30"))


# ===================== HELPERS ===================== #
def norm_col(s: str) -> str:
    # normalize column names: "change%" "Change %" "CHANGE %" -> "change%"
    return re.sub(r"\s+", "", str(s).strip().lower())

def find_col(df: pd.DataFrame, candidates):
    # returns actual column name from df that matches any candidate after normalization
    norm_map = {norm_col(c): c for c in df.columns}
    for cand in candidates:
        key = norm_col(cand)
        if key in norm_map:
            return norm_map[key]
    # also try fuzzy: contains
    for cand in candidates:
        key = norm_col(cand)
        for k, real in norm_map.items():
            if k == key or key in k or k in key:
                return real
    return None

def parse_percent_any(val) -> float:
    """
    Returns a float number (no % sign), e.g.
      "7%" -> 7.0
      "0.07" -> 0.07
      "-1.2%" -> -1.2
      ""/None/"-" -> None
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "-", "—"):
        return None
    s = s.replace(",", "")
    s = s.replace("%", "")
    m = re.search(r"-?\d+(\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except:
        return None

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

def focus_and_send_timeframe(driver, chart_el, tf_text: str):
    # click chart to ensure keyboard shortcuts go to chart
    try:
        ActionChains(driver).move_to_element(chart_el).click(chart_el).perform()
        time.sleep(0.3)
    except:
        pass
    ActionChains(driver).send_keys(tf_text).send_keys(Keys.ENTER).perform()


def infer_scale_multiplier(values):
    """
    If sheet stores 0.07 for 7% then numbers are <= 1 mostly.
    We infer:
      - if max_abs <= 1.5 => treat as fraction, multiply by 100 to get percent
      - else treat as already percent
    """
    nums = []
    for v in values:
        x = parse_percent_any(v)
        if x is None:
            continue
        nums.append(abs(x))
    if not nums:
        return 1.0
    mx = max(nums)
    return 100.0 if mx <= 1.5 else 1.0


# ===================== MAIN ===================== #
def main():
    clear_db_before_run()

    # --- Load Google Sheets ---
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

        # Map: col A => symbol, col C => TV url (same as your original)
        link_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))

        print(f"📄 MV2 rows: {len(df_mv2)} | StockList rows: {len(df_stocks)}", flush=True)

    except Exception as e:
        print(f"❌ Sheet Error: {e}", flush=True)
        return

    # --- Find columns (robust) ---
    sym_col = find_col(df_mv2, ["Symbol", "SYMBOL"])
    sec_col = find_col(df_mv2, ["Sector", "SECTOR"])
    d_col   = find_col(df_mv2, ["change%", "change %", "Change%", "Change %"])
    m_col   = find_col(df_mv2, ["mchange%", "mchange %", "Mchange%", "MChange%", "mChange%", "MonthlyChange%", "monthlychange%"])

    if not sym_col:
        print("❌ Cannot find Symbol column in MV2 sheet.", flush=True)
        print(f"Available columns: {list(df_mv2.columns)}", flush=True)
        return
    if not d_col or not m_col:
        print("❌ Cannot find change% / mchange% columns in MV2 sheet.", flush=True)
        print(f"Available columns: {list(df_mv2.columns)}", flush=True)
        return

    # --- Infer scale (percent vs fraction) ---
    d_mult = infer_scale_multiplier(df_mv2[d_col].tolist())
    m_mult = infer_scale_multiplier(df_mv2[m_col].tolist())
    print(f"🔎 Detected scale: change% x{d_mult} | mchange% x{m_mult}", flush=True)
    print(f"📌 Rules: daily >= {DAILY_THRESHOLD_PCT}% OR monthly >= {MONTHLY_THRESHOLD_PCT}%", flush=True)

    # --- Browser ---
    driver = get_driver()
    if not inject_tv_cookies(driver):
        try: driver.quit()
        except: pass
        return

    processed = 0
    saved = 0
    matched = 0

    for _, row in df_mv2.iterrows():
        symbol = str(row.get(sym_col, "")).strip()
        if not symbol:
            continue

        # sector rejection (same as your original)
        sector = str(row.get(sec_col, "")).strip().upper() if sec_col else ""
        if sector in ("INDICES", "MUTUAL FUND SCHEME"):
            continue

        d_raw = row.get(d_col, None)
        m_raw = row.get(m_col, None)
        d_val = parse_percent_any(d_raw)
        m_val = parse_percent_any(m_raw)
        if d_val is None and m_val is None:
            continue

        # convert to percent units
        daily_pct = (d_val * d_mult) if d_val is not None else 0.0
        monthly_pct = (m_val * m_mult) if m_val is not None else 0.0

        # apply rules
        if daily_pct < DAILY_THRESHOLD_PCT and monthly_pct < MONTHLY_THRESHOLD_PCT:
            continue

        matched += 1

        url = link_map.get(symbol)
        if not url or "tradingview.com" not in url:
            continue

        processed += 1
        print(f"➡️ {symbol} | change%={daily_pct:.4f} | mchange%={monthly_pct:.4f}", flush=True)

        try:
            driver.get(url)
            time.sleep(PAGE_LOAD_SLEEP)

            # chart element (kept like your original)
            chart = WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.visibility_of_element_located(
                    (By.XPATH, "//div[contains(@class, 'chart-container')]")
                )
            )

            time.sleep(CHART_PAINT_SLEEP)

            if daily_pct >= DAILY_THRESHOLD_PCT:
                focus_and_send_timeframe(driver, chart, "1D")
                time.sleep(TF_SLEEP)
                save_to_mysql(symbol, "daily", chart.screenshot_as_png)
                saved += 1

            if monthly_pct >= MONTHLY_THRESHOLD_PCT:
                focus_and_send_timeframe(driver, chart, "1M")
                time.sleep(TF_SLEEP)
                save_to_mysql(symbol, "monthly", chart.screenshot_as_png)
                saved += 1

        except Exception as e:
            print(f"⚠️ Screenshot Error ({symbol}): {e}", flush=True)

    try: driver.quit()
    except: pass

    print(f"🏁 DONE! matched={matched} processed={processed} saved={saved}", flush=True)


if __name__ == "__main__":
    main()
