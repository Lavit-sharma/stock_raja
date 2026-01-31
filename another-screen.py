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

# ---------------- CONFIG ---------------- #
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"
MAX_THREADS = 4 

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

db_pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="screenshot_pool",
    pool_size=MAX_THREADS + 2,
    **DB_CONFIG
)

# ---------------- HELPERS ---------------- #

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
    except mysql.connector.Error as err:
        print(f"    ❌ DB Error for {symbol}: {err}")

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        for c in cookies:
            driver.add_cookie({
                "name": c.get("name"),
                "value": c.get("value"),
                "domain": ".tradingview.com",
                "path": "/"
            })
        driver.refresh()
        return True
    except:
        return False

def navigate_and_snap(driver, symbol, timeframe, url, target_date, month_val):
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 25)
        chart = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]")))

        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(2) # Increased for stability
        
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()

        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = wait.until(EC.visibility_of_element_located((By.XPATH, input_xpath)))

        goto_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        goto_input.send_keys(str(target_date) + Keys.ENTER)

        time.sleep(7) # Give chart time to load the specific date

        img = chart.screenshot_as_png
        save_to_mysql(symbol, timeframe, img, target_date, month_val)
        print(f"✅ Captured {symbol} ({timeframe}) | {month_val}")
    except Exception as e:
        print(f"⚠️ Failed {symbol} ({timeframe}): {str(e)[:100]}")

def process_row(row):
    # Use lowercase keys to be safe with column naming
    row_lower = {str(k).lower().strip(): v for k, v in row.items()}
    
    symbol = str(row_lower.get('symbol', '')).strip()
    day_url = str(row_lower.get('day', '')).strip()
    target_date = str(row_lower.get('dates', '')).strip()

    # DEBUG: Help you see why it might be skipping
    if not symbol or not day_url or not target_date:
        # print(f"DEBUG: Skipping row - Missing data: Sym:{symbol}, URL:{day_url}, Date:{target_date}")
        return

    if "tradingview.com" not in day_url:
        return

    month_val = get_month_name(target_date)
    driver = get_driver()

    try:
        if inject_tv_cookies(driver):
            navigate_and_snap(driver, symbol, "day", day_url, target_date, month_val)
    except Exception as e:
        print(f"Driver Error for {symbol}: {e}")
    finally:
        driver.quit()

# ---------------- MAIN ---------------- #

def main():
    rows = []
    max_retries = 5

    for attempt in range(max_retries):
        try:
            print(f"🔄 Fetching Data (Attempt {attempt + 1})...")
            creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
            gc = gspread.service_account_from_dict(creds)
            spreadsheet = gc.open(SPREADSHEET_NAME)
            worksheet = spreadsheet.worksheet(TAB_NAME)
            all_values = worksheet.get_all_values()

            if all_values:
                headers = [h.strip() for h in all_values[0]]
                df = pd.DataFrame(all_values[1:], columns=headers)
                
                # Remove duplicate columns (keeps the first occurrence)
                df = df.loc[:, ~df.columns.duplicated()].copy()
                
                rows = df.to_dict('records')
                print(f"✅ Loaded {len(rows)} symbols.")
                break
        except Exception as e:
            print(f"⚠️ Error: {e}")
            time.sleep(5)

    if not rows:
        print("❌ No data found to process.")
        return

    # Use list() to force the executor to actually complete the work
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_row, row) for row in rows]
        # This waits for each thread to finish and reports errors
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Thread Error: {e}")

    print("🏁 Processing Finished.")

if __name__ == "__main__":
    main()
