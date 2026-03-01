import os, time, json, gspread, concurrent.futures, re, traceback
import pandas as pd
import mysql.connector
from mysql.connector import pooling
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"
MAX_THREADS = int(os.getenv("MAX_THREADS", "4"))  # can override via env

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}

# Connection Pool for high-speed DB inserts
db_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="screenshot_pool",
    pool_size=MAX_THREADS + 2,
    **DB_CONFIG
)

# ✅ Resolve chromedriver ONCE
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- LOGGING ---------------- #
RUN_ID = datetime.now().strftime("%Y%m%d-%H%M%S")

def log(msg, symbol="-", tf="-"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{RUN_ID}] [{symbol}] [{tf}] {msg}", flush=True)

def short_exc(e: Exception, max_len=160):
    s = f"{type(e).__name__}: {e}"
    return (s[:max_len] + "...") if len(s) > max_len else s

# ---------------- HELPERS ---------------- #

def make_unique_headers(headers):
    """Make headers unique (fixes Pandas non-unique columns warning)."""
    seen = {}
    out = []
    for h in headers:
        key = (h or "").strip()
        if key == "":
            key = "col"
        if key in seen:
            seen[key] += 1
            out.append(f"{key}_{seen[key]}")
        else:
            seen[key] = 1
            out.append(key)
    return out

def get_month_name(date_str):
    try:
        clean_date = re.sub(r'[*]', '', str(date_str)).strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(clean_date, fmt)
                return dt.strftime('%B')
            except ValueError:
                continue
        return "Unknown"
    except:
        return "Unknown"

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    try:
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date, month_before) 
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                month_before = VALUES(month_before),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, chart_date, month_val))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except mysql.connector.Error as err:
        log(f"❌ DB Error: {err}", symbol, timeframe)
        return False

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")

    # stability
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-renderer-backgrounding")

    service = Service(CHROME_DRIVER_PATH)
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver, symbol="-"):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            log("⚠️ TRADINGVIEW_COOKIES env is empty", symbol, "-")
            return False

        cookies = json.loads(cookie_data)
        log("🌐 Visiting: https://www.tradingview.com/ (cookie inject)", symbol, "-")
        driver.get("https://www.tradingview.com/")

        added = 0
        for c in cookies:
            name = c.get("name")
            value = c.get("value")
            if not name or value is None:
                continue
            driver.add_cookie({
                "name": name,
                "value": value,
                "domain": ".tradingview.com",
                "path": "/",
            })
            added += 1

        driver.refresh()
        log(f"✅ Cookies injected: {added}", symbol, "-")
        return added > 0
    except Exception as e:
        log(f"⚠️ Cookie inject failed: {short_exc(e)}", symbol, "-")
        return False

def _wait_chart_ready(driver, timeout=30, symbol="-", tf="-"):
    wait = WebDriverWait(driver, timeout)
    chart_xpath = "//div[contains(@class,'chart-container') or contains(@class,'chart')]"
    canvas_xpath = f"{chart_xpath}//canvas"
    loader_xpaths = [
        "//*[contains(@class,'loader') and not(contains(@style,'display: none'))]",
        "//*[contains(@class,'spinner') and not(contains(@style,'display: none'))]",
        "//*[contains(@class,'tv-spinner') and not(contains(@style,'display: none'))]",
        "//*[contains(@class,'loading') and not(contains(@style,'display: none'))]",
        "//*[@role='progressbar']",
    ]

    wait.until(EC.presence_of_element_located((By.XPATH, chart_xpath)))

    end = time.time() + timeout
    while time.time() < end:
        try:
            canvases = driver.find_elements(By.XPATH, canvas_xpath)
            has_canvas = len(canvases) > 0
            loader_visible = False
            for lx in loader_xpaths:
                for el in driver.find_elements(By.XPATH, lx):
                    try:
                        if el.is_displayed():
                            loader_visible = True
                            break
                    except:
                        continue
                if loader_visible:
                    break

            if has_canvas and not loader_visible:
                time.sleep(0.5)  # settle render
                return True
        except:
            pass
        time.sleep(0.4)

    log("⚠️ Chart not ready (timeout)", symbol, tf)
    return False

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
        log(f"🌐 Visiting: {url}", symbol, timeframe)
        driver.get(url)

        if not _wait_chart_ready(driver, timeout=35, symbol=symbol, tf=timeframe):
            raise Exception("Chart not ready after page load")

        wait = WebDriverWait(driver, 25)
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class,'chart-container') or contains(@class,'chart')]")))
        ActionChains(driver).move_to_element(chart).click().perform()

        # open Go-to-date (ALT+G)
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()

        input_xpath = "//input[contains(@class,'query') or @data-role='search' or contains(@class,'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))

        log(f"⌨️ GoToDate: {target_date}", symbol, timeframe)
        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(str(target_date))
        goto_input.send_keys(Keys.ENTER)

        # wait input closes
        try:
            wait.until(EC.staleness_of(goto_input))
        except:
            try:
                wait.until(EC.invisibility_of_element_located((By.XPATH, input_xpath)))
            except:
                pass

        if not _wait_chart_ready(driver, timeout=40, symbol=symbol, tf=timeframe):
            raise Exception("Chart not ready after goto-date")

        # ✅ NEW: Added extra waiting time to allow values/bars to fully render before screenshot
        log(f"⏳ Waiting 5s for values to load...", symbol, timeframe)
        time.sleep(5)

        img = chart.screenshot_as_png
        ok = save_to_mysql(symbol, timeframe, img, target_date, month_val)
        if ok:
            log(f"✅ Screenshot saved | month_before={month_val}", symbol, timeframe)
        else:
            log("⚠️ Screenshot captured but DB save failed", symbol, timeframe)

    except Exception as e:
        log(f"❌ Failed: {short_exc(e)}", symbol, timeframe)

def process_row(row, idx):
    symbol = str(row.get("Symbol", "")).strip()
    week_url = str(row.get("Week", "")).strip()
    day_url  = str(row.get("Day", "")).strip()
    target_date = str(row.get("dates", "")).strip()

    if not symbol:
        log(f"⏭️ Skip row#{idx}: empty Symbol", "-", "-")
        return

    if not re.search(r"\d", target_date):
        log(f"⏭️ Skip {symbol}: invalid dates='{target_date}'", symbol, "-")
        return

    if not day_url and not week_url:
        log(f"⏭️ Skip {symbol}: Day & Week URLs empty", symbol, "-")
        return

    month_val = get_month_name(target_date)

    driver = get_driver()
    try:
        ok_cookie = inject_tv_cookies(driver, symbol=symbol)
        if not ok_cookie:
            log("⏭️ Skip (cookie not injected)", symbol, "-")
            return

        if day_url and "tradingview.com" in day_url:
            navigate_and_snap(driver, symbol, "day", day_url, target_date, month_val)
        
        if week_url and "tradingview.com" in week_url:
            navigate_and_snap(driver, symbol, "week", week_url, target_date, month_val)

    finally:
        try:
            driver.quit()
        except:
            pass

# ---------------- MAIN ---------------- #

def main():
    # ✅ NEW: Truncate table before starting the crawl
    try:
        log("🧹 Truncating table 'another_screenshot' before processing...", "-", "-")
        conn = db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE another_screenshot")
        conn.commit()
        cursor.close()
        conn.close()
        log("✅ Table truncated successfully.", "-", "-")
    except Exception as e:
        log(f"⚠️ Failed to truncate table: {short_exc(e)}", "-", "-")

    rows = []
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            log(f"🔄 Fetching Google Sheet (Attempt {attempt+1}/{max_retries})", "-", "-")
            creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
            gc = gspread.service_account_from_dict(creds)
            spreadsheet = gc.open(SPREADSHEET_NAME)
            worksheet = spreadsheet.worksheet(TAB_NAME)

            all_values = worksheet.get_all_values()
            if not all_values or len(all_values) < 2:
                log("⚠️ Sheet has no data rows", "-", "-")
                return

            raw_headers = all_values[0]
            headers = make_unique_headers([str(h) for h in raw_headers])
            df = pd.DataFrame(all_values[1:], columns=headers)
            rows = df.to_dict("records")

            log(f"✅ Loaded rows: {len(rows)}", "-", "-")
            break

        except Exception as e:
            if attempt < max_retries - 1:
                log(f"⚠️ Google API Error: {short_exc(e)} | retry in {retry_delay}s", "-", "-")
                time.sleep(retry_delay)
            else:
                log(f"❌ Final Google Sheet Error: {short_exc(e)}", "-", "-")
                return

    if not rows:
        log("❌ No rows to process", "-", "-")
        return

    log(f"🚀 Starting workers: {MAX_THREADS}", "-", "-")
    indexed_rows = list(enumerate(rows, start=1))

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_row, row, idx) for idx, row in indexed_rows]
        for f in concurrent.futures.as_completed(futures):
            try:
                f.result()
            except Exception as e:
                log(f"❌ Thread crashed: {short_exc(e)}", "-", "-")

    log("🏁 Processing Finished.", "-", "-")

if __name__ == "__main__":
    main()
