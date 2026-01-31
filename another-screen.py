import os, time, json, gspread, concurrent.futures, re
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
from datetime import datetime
import threading

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"
MAX_THREADS = 4 

progress_lock = threading.Lock()
processed_count = 0
total_rows = 0

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 30
}

db_pool = None

def init_db_pool():
    global db_pool
    try:
        print("📡 Flag: Connecting to Database...")
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool",
            pool_size=MAX_THREADS + 2,
            **DB_CONFIG
        )
        print("✅ FLAG: DATABASE CONNECTION SUCCESSFUL")
        return True
    except Exception as e:
        print(f"❌ FLAG: DATABASE CONNECTION FAILED: {e}")
        return False

# ---------------- HELPERS ---------------- #

def save_to_mysql(symbol, timeframe, image_data, chart_date, month_val):
    if db_pool is None: return False
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
    except Exception as err:
        print(f"    ❌ DB SAVE ERROR [{symbol}]: {err}")
        return False

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=opts)

def process_row(row):
    global processed_count
    row_clean = {str(k).lower().strip(): v for k, v in row.items()}
    symbol = str(row_clean.get('symbol', '')).strip()
    day_url = str(row_clean.get('day', '')).strip()
    target_date = str(row_clean.get('dates', '')).strip()

    with progress_lock:
        processed_count += 1
        current_idx = processed_count

    if not symbol or "tradingview.com" not in day_url:
        print(f"⏩ [{current_idx}/{total_rows}] Flag: Skipping {symbol or 'Unknown'}")
        return

    print(f"🚀 [{current_idx}/{total_rows}] Flag: Starting {symbol}...")
    
    driver = get_driver()
    try:
        # 1. Inject Cookies
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        driver.get("https://www.tradingview.com/")
        if cookie_data:
            for c in json.loads(cookie_data):
                driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
            driver.refresh()

        # 2. Open Chart
        driver.get(day_url)
        wait = WebDriverWait(driver, 25)
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]")))
        
        # --- FIX: HIDE ADS/POPUPS ---
        try:
            # This JavaScript finds common TradingView overlay containers and hides them
            driver.execute_script("""
                var selectors = [
                    "div[class*='overlap-manager']", 
                    "div[class*='dialog-']", 
                    "button[aria-label='Close']",
                    "div[class*='toast-container']",
                    "div[class*='notification-']"
                ];
                selectors.forEach(function(s) {
                    var els = document.querySelectorAll(s);
                    els.forEach(function(e) { e.style.display = 'none'; });
                });
            """)
        except: pass
        # ----------------------------

        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))
        goto_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        goto_input.send_keys(target_date + Keys.ENTER)
        
        time.sleep(6)

        # FINAL CHECK: Hide ad again right before snap if it reappeared
        driver.execute_script("document.querySelectorAll(\"div[class*='overlap-manager']\").forEach(e => e.style.display = 'none');")
        
        img = chart.screenshot_as_png
        
        # 3. Save
        month_val = "Unknown"
        try:
            month_val = datetime.strptime(re.sub(r'[*]', '', target_date).strip(), "%Y-%m-%d").strftime('%B')
        except: pass

        if save_to_mysql(symbol, "day", img, target_date, month_val):
            print(f"✅ [{current_idx}/{total_rows}] FLAG: SUCCESS - {symbol}")
        else:
            print(f"❌ [{current_idx}/{total_rows}] FLAG: FAILED - {symbol}")

    except Exception as e:
        print(f"⚠️ [{current_idx}/{total_rows}] FLAG: ERROR {symbol}: {str(e)[:50]}")
    finally:
        driver.quit()

# ---------------- MAIN ---------------- #

def main():
    global total_rows
    if not init_db_pool(): return 

    try:
        print("📑 Flag: Connecting to Google Sheets...")
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        all_values = worksheet.get_all_values()
        
        headers = [h.strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        rows = df.to_dict('records')
        total_rows = len(rows)
        print(f"✅ FLAG: GOOGLE SHEETS CONNECTED. Found {total_rows} rows.")
    except Exception as e:
        print(f"❌ FLAG: GOOGLE SHEETS ERROR: {e}")
        return

    print(f"⚙️ Flag: Processing started with {MAX_THREADS} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        list(executor.map(process_row, rows))

    print("\n🏁 FLAG: ALL TASKS FINISHED.")

if __name__ == "__main__":
    main()
