import os
import time
import json
from datetime import datetime
import gspread
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

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "connect_timeout": 15
}

def get_driver():
    """Initializes a production-grade headless Chrome driver."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver

def save_to_db(symbol, timeframe, img_data, chart_date):
    """Saves image and the specific date from the sheet to MySQL."""
    if not img_data or len(img_data) < 1000:
        print(f"⚠️  [DB SAVE] Invalid or empty screenshot data for {symbol} ({timeframe}).")
        return False
        
    conn = None
    try:
        print(f"🔌  [DB SAVE] Connecting to database to save screenshot for {symbol}...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, img_data, chart_date))
        conn.commit()
        cursor.close()
        print(f"💾  [DB SAVE] Successfully saved screenshot for {symbol} ({timeframe}) into 'another_screenshot'.")
        return True
    except Exception as e:
        print(f"❌  [DB SAVE ERROR] Failed to save {symbol} ({timeframe}): {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

def calculate_and_save_daily_sum():
    """
    Fetches CURR_DQ and D_CLOSE from wp_mv2, multiplies them row-by-row,
    and upserts the accumulated matrix sum into the closesum table tagged by current date.
    """
    print("\n" + "="*60)
    print("🧮  STARTING DAY-WISE VALUE SUMMATION CALCULATION ROUTINE")
    print("="*60)
    
    today_date = datetime.now().strftime('%Y-%m-%d')
    print(f"📆  Targeting Execution Date: {today_date}")
    
    conn = None
    try:
        print("🔌  [DB MATH] Connecting to database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        print("✅  [DB MATH] Database connected successfully.")
        cursor = conn.cursor(dictionary=True)

        # 1. Fetch the target data columns from wp_mv2
        print("📥  [DB MATH] Fetching all rows (Symbol, CURR_DQ, D_CLOSE) from 'wp_mv2' table...")
        cursor.execute("SELECT Symbol, CURR_DQ, D_CLOSE FROM wp_mv2")
        rows = cursor.fetchall()
        print(f"📋  [DB MATH] Successfully fetched {len(rows)} records from 'wp_mv2'.")

        grand_total = 0.0
        processed_count = 0

        # 2. Row-by-Row multiplication and compounding
        print("⚙️  [DB MATH] Beginning row-by-row multiplication (CURR_DQ * D_CLOSE)...")
        for idx, row in enumerate(rows, start=1):
            symbol = row.get("Symbol", "UNKNOWN")
            curr_dq_str = row.get("CURR_DQ")
            d_close_str = row.get("D_CLOSE")

            if curr_dq_str is not None and d_close_str is not None:
                try:
                    # Clean strings (remove commas, spaces, currency symbols) and convert to float
                    curr_dq = float(str(curr_dq_str).replace(",", "").strip())
                    d_close = float(str(d_close_str).replace(",", "").strip())
                    
                    # Row multiplication
                    row_product = curr_dq * d_close
                    grand_total += row_product
                    processed_count += 1
                    
                    # Periodic summary logging every 100 rows
                    if idx % 100 == 0 or idx == len(rows):
                        print(f"    ↳ Processing row {idx}/{len(rows)} | Current Accumulated Sum: {grand_total:,.2f}")
                        
                except ValueError:
                    # Skip problematic text inputs gracefully
                    continue
            else:
                if idx % 500 == 0:
                    print(f"    ↳ Line item status check: Row {idx}/{len(rows)} processed.")

        print("-"*60)
        print(f"📊  [DB MATH SUMMARY] Calculated {processed_count} valid rows successfully.")
        print(f"💎  [DB MATH SUMMARY] Final Generated Sum Product: {grand_total:,.2f}")
        print("-"*60)

        # 3. Save the final calculated value to closesum table matching today's date
        print(f"📤  [DB MATH] Upserting daily total value into 'closesum' for date: {today_date}...")
        save_query = """
            INSERT INTO closesum (calculation_date, total_dq_value) 
            VALUES (%s, %s) 
            ON DUPLICATE KEY UPDATE 
                total_dq_value = VALUES(total_dq_value)
        """
        cursor.execute(save_query, (today_date, str(grand_total)))
        conn.commit()
        print(f"🚀  [DB MATH] Successfully processed and recorded metrics for context date {today_date}!")

        cursor.close()
    except Exception as e:
        print(f"❌  [DB MATH GLOBAL ERROR] Critical failure during execution context: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("🔌  [DB MATH] Closed database connection pipeline safely.")
    print("="*60 + "\n")

def process_row(row):
    """Handles logic using exact headers: Symbol, Week, Day, and dates."""
    clean_row = {str(k).strip(): v for k, v in row.items()}
    
    symbol = str(clean_row.get("Symbol", "")).strip()
    target_date = str(clean_row.get("dates", "")).strip()

    urls_to_process = {
        "week": str(clean_row.get("Week", "")).strip(),
        "day": str(clean_row.get("Day", "")).strip()
    }

    if not symbol or not target_date:
        print(f"⚠️  [SCRAPER] Skipping Google Sheet Row: Missing tracking configuration criteria (Symbol/Dates).")
        return

    for timeframe, url in urls_to_process.items():
        if not url or "tradingview.com" not in url:
            print(f"⚠️  [SCRAPER] Skipping {symbol} ({timeframe}): Missing/invalid TradingView reference URL.")
            continue

        print(f"🌐  [SCRAPER] Active Target -> Symbol: {symbol} | Timeframe: {timeframe} | Target Date: {target_date}")
        driver = get_driver()
        
        try:
            driver.get("https://www.tradingview.com/")
            cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]"))
            for c in cookies:
                try:
                    driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
                except: continue
            
            driver.get(url)
            wait = WebDriverWait(driver, 35)
            
            chart_xpath = "//div[contains(@class,'chart-container') or contains(@class,'chart-gui-wrapper')]"
            chart = wait.until(EC.presence_of_element_located((By.XPATH, chart_xpath)))
            ActionChains(driver).move_to_element(chart).click().perform()
            time.sleep(2)
            
            ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()
            
            input_xpath = "//input[contains(@class,'query') or contains(@class,'input')]"
            date_input = wait.until(EC.element_to_be_clickable((By.XPATH, input_xpath)))
            date_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
            time.sleep(0.5)
            date_input.send_keys(target_date + Keys.ENTER)
            
            print(f"    📍 Jumped to calendar viewport reference date: {target_date} ({timeframe} chart).")
            print("    ⏳ Waiting 12 seconds for indicators and historical ticks to render standard assets...")
            time.sleep(12)

            driver.execute_script("""
                document.querySelectorAll('[class*="overlap-"], [class*="modal-"], [class*="dialog-"], .tv-dialog__close').forEach(el => el.remove());
            """)
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(1)
            
            img = driver.get_screenshot_as_png()
            if save_to_db(symbol, timeframe, img, target_date):
                print(f"    ✅ Captured and recorded snapshot for {symbol} successfully.")
            
        except Exception as e:
            print(f"❌  [SCRAPER ERROR] Exception hit processing {symbol} ({timeframe}): {str(e)[:100]}")
        finally:
            driver.quit()
        
        time.sleep(2)

def main():
    print("🏁  [STARTUP] Launching automation script sequence...")
    try:
        print("📥  [GSPREAD] Accessing workspace credentials payload configuration...")
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        print("📖  [GSPREAD] Pulling records from Google Sheet: 'Stock List' -> Worksheet: 'Weekday'...")
        sh = gc.open("Stock List").worksheet("Weekday")
        rows = sh.get_all_records()
        print(f"✅  [GSPREAD] Total rows loaded from Google Sheet: {len(rows)}")
    except Exception as e:
        print(f"❌  [GSPREAD CRITICAL ERROR] Initialization pipeline broken: {e}")
        return

    start = int(os.getenv("START_ROW", 0))
    end = int(os.getenv("END_ROW", 500))
    
    print(f"⚙️  [SCHEDULER] Range configured from environment parameters: Row {start} to Row {end}.")
    
    # 1. Run automation loop
    for idx, row in enumerate(rows[start:end], start=start+1):
        print(f"\n📝  [PROCESS] Handling Data Sheet Segment Entry index #{idx}")
        process_row(row)
        time.sleep(1)

    print("\n🏁  [SCRAPER COMPLETE] Finished cloud automation processing workspace loop records.")

    # 2. Run the Day-wise calculations using the new closesum table
    calculate_and_save_daily_sum()

if __name__ == "__main__":
    main()
