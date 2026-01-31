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
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import threading

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"
MAX_THREADS = 4 

# Thread-safe counter for progress tracking
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
        print("📡 Attempting to connect to Database...")
        db_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="screenshot_pool",
            pool_size=MAX_THREADS + 2,
            **DB_CONFIG
        )
        print("✅ DATABASE CONNECTION SUCCESSFUL")
        return True
    except Exception as e:
        print(f"❌ DATABASE CONNECTION FAILED: {e}")
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
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

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
        print(f"⏩ [{current_idx}/{total_rows}] Skipping {symbol or 'Unknown'}: Missing URL/Data")
        return

    print(f"🚀 [{current_idx}/{total_rows}] Starting: {symbol} at {target_date}")
    
    driver = get_driver()
    try:
        # 1. Login/Cookies
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        driver.get("https://www.tradingview.com/")
        if cookie_data:
            for c in json.loads(cookie_data):
                driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
            driver.refresh()

        # 2. Navigate and Capture
        driver.get(day_url)
        wait = WebDriverWait(driver, 20)
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]")))
        
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))
        goto_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        goto_input.send_keys(target_date + Keys.ENTER)
        
        time.sleep(6) # Wait for chart to move
        
        img = chart.screenshot_as_png
        
        # 3. Database Save
        month_val = "Unknown"
        try:
            month_val = datetime.strptime(re.sub(r'[*]', '', target_date).strip(), "%Y-%m-%d").strftime('%B')
        except: pass

        if save_to_mysql(symbol, "day", img, target_date, month_val):
            print(f"✅ [{current_idx}/{total_rows}] SUCCESS: {symbol} Screenshot Saved to DB")
        else:
            print(f"❌ [{current_idx}/{total_rows}] FAILED: {symbol} Database Error")

    except Exception as e:
        print(f"⚠️ [{current_idx}/{total_rows}] ERROR {symbol}: {str(e)[:50]}")
    finally:
        driver.quit()

# ---------------- MAIN ---------------- #

def main():
    global total_rows
    
    # Flag 1: Database Check
    if not init_db_pool():
        print("🛑 FATAL: Could not connect to your Database. Script stopping.")
        return 

    # Flag 2: Google Sheets Check
    try:
        print("📑 Connecting to Google Sheets...")
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        all_values = worksheet.get_all_values()
        
        if not all_values:
            print("❌ Sheet is empty!")
            return

        headers = [h.strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
        df = df.loc[:, ~df.columns.duplicated()].copy()
        rows = df.to_dict('records')
        total_rows = len(rows)
        print(f"✅ GOOGLE SHEETS CONNECTED: {total_rows} symbols loaded.")
        
    except Exception as e:
        print(f"❌ GOOGLE SHEETS ERROR: {e}")
        return

    # Flag 3: Process Starting
    print(f"⚙️ Starting Process with {MAX_THREADS} parallel workers...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        list(executor.map(process_row, rows))

    print("\n🏁 ALL TASKS COMPLETED.")

if __name__ == "__main__":
    main()
