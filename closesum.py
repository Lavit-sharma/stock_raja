import os
import time
import json
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
        return False
        
    conn = None
    try:
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
        return True
    except Exception as e:
        print(f"❌ DB Error for {symbol} ({timeframe}): {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

def ensure_column_exists_and_save_total():
    """
    Fetches CURR_DQ and D_CLOSE from wp_mv2, multiplies them row-by-row,
    calculates the grand sum, ensures the destination column exists in wp_live_close,
    and updates/saves the combined sum into wp_live_close.
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # 1. Ensure the calculation column exists in the target table wp_live_close
        print("🛠️ Checking/Adding output column in 'wp_live_close'...")
        try:
            cursor.execute("""
                ALTER TABLE wp_live_close 
                ADD COLUMN total_dq_value VARCHAR(255) NULL DEFAULT NULL
            """)
            conn.commit()
            print("✅ Column 'total_dq_value' successfully added to wp_live_close.")
        except mysql.connector.Error as err:
            if err.errno == 1060:  # Duplicate column name error code
                print("ℹ️ Column 'total_dq_value' already exists.")
            else:
                raise err

        # 2. Fetch the target data columns from wp_mv2
        print("📊 Fetching CURR_DQ and D_CLOSE from wp_mv2...")
        cursor.execute("SELECT Symbol, CURR_DQ, D_CLOSE FROM wp_mv2")
        rows = cursor.fetchall()

        grand_total = 0.0
        processed_count = 0

        # 3. Row-by-Row multiplication and compounding
        for row in rows:
            curr_dq_str = row.get("CURR_DQ")
            d_close_str = row.get("D_CLOSE")

            if curr_dq_str is not None and d_close_str is not None:
                try:
                    # Clean strings (remove commas, spaces, etc.) and convert to float
                    curr_dq = float(str(curr_dq_str).replace(",", "").strip())
                    d_close = float(str(d_close_str).replace(",", "").strip())
                    
                    # Row multiplication
                    row_product = curr_dq * d_close
                    grand_total += row_product
                    processed_count += 1
                except ValueError:
                    # Skipping entries that aren't valid numbers (like empty strings or headers)
                    continue

        print(f"📈 Summed up {processed_count} valid rows. Grand Total Product: {grand_total}")

        # 4. Save the final calculated value to wp_live_close
        # Using a row lookup or baseline record identifier if applicable. 
        # Here it updates the global state or first record for simple storage.
        save_query = """
            INSERT INTO wp_live_close (id, total_dq_value) 
            VALUES (1, %s) 
            ON DUPLICATE KEY UPDATE total_dq_value = VALUES(total_dq_value)
        """
        cursor.execute(save_query, (str(grand_total),))
        conn.commit()
        print("🚀 Successfully updated the total sum product into wp_live_close!")

        cursor.close()
    except Exception as e:
        print(f"❌ Error during Database Math Processing: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

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
        print(f"⚠️ Skipping row: Missing Symbol or dates info.")
        return

    for timeframe, url in urls_to_process.items():
        if not url or "tradingview.com" not in url:
            print(f"⚠️ Skipping {symbol} ({timeframe}): Invalid or missing URL.")
            continue

        print(f"🚀 Processing: {symbol} | Timeframe: {timeframe} | Target Date: {target_date}")
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
            
            print(f"📍 Jumped to {target_date} on {timeframe} chart.")
            time.sleep(12)

            driver.execute_script("""
                document.querySelectorAll('[class*="overlap-"], [class*="modal-"], [class*="dialog-"], .tv-dialog__close').forEach(el => el.remove());
            """)
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(1)
            
            img = driver.get_screenshot_as_png()
            if save_to_db(symbol, timeframe, img, target_date):
                print(f"✅ Saved {symbol} for {target_date} ({timeframe})")
            
        except Exception as e:
            print(f"❌ Error during {symbol} ({timeframe}): {str(e)[:100]}")
        finally:
            driver.quit()
        
        time.sleep(2)

def main():
    # 1. Execute the spreadsheet automation routine
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open("Stock List").worksheet("Weekday")
        rows = sh.get_all_records()
    except Exception as e:
        print(f"❌ Spreadsheet Error: {e}")
        return

    start = int(os.getenv("START_ROW", 0))
    end = int(os.getenv("END_ROW", 500))
    
    for row in rows[start:end]:
        process_row(row)
        time.sleep(1)

    # 2. Execute database calculation logic once automation finishes
    ensure_column_exists_and_save_total()

if __name__ == "__main__":
    main()
