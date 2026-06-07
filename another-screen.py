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

def process_row(row):
    """Handles logic using exact headers: Symbol, Week, Day, and dates."""
    # Strip whitespace from keys to prevent 'KeyError' from hidden spaces in Sheet
    clean_row = {str(k).strip(): v for k, v in row.items()}
    
    symbol = str(clean_row.get("Symbol", "")).strip()
    target_date = str(clean_row.get("dates", "")).strip()

    # Map timeframes directly to your new sheet column headers
    urls_to_process = {
        "week": str(clean_row.get("Week", "")).strip(),
        "day": str(clean_row.get("Day", "")).strip()
    }

    if not symbol or not target_date:
        print(f"⚠️ Skipping row: Missing Symbol or dates info.")
        return

    # Loop through both URLs sequentially
    for timeframe, url in urls_to_process.items():
        if not url or "tradingview.com" not in url:
            print(f"⚠️ Skipping {symbol} ({timeframe}): Invalid or missing URL.")
            continue

        print(f"🚀 Processing: {symbol} | Timeframe: {timeframe} | Target Date: {target_date}")
        driver = get_driver()
        
        try:
            # 1. Login via Cookies
            driver.get("https://www.tradingview.com/")
            cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES", "[]"))
            for c in cookies:
                try:
                    driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
                except: continue
            
            # 2. Open Chart (Can be Week or Day URL)
            driver.get(url)
            wait = WebDriverWait(driver, 35)
            
            # 3. Focus and Go To Date
            chart_xpath = "//div[contains(@class,'chart-container') or contains(@class,'chart-gui-wrapper')]"
            chart = wait.until(EC.presence_of_element_located((By.XPATH, chart_xpath)))
            ActionChains(driver).move_to_element(chart).click().perform()
            time.sleep(2)
            
            # Alt + G Shortcut
            ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()
            
            # 4. Input the 'dates' value from your sheet
            input_xpath = "//input[contains(@class,'query') or contains(@class,'input')]"
            date_input = wait.until(EC.element_to_be_clickable((By.XPATH, input_xpath)))
            date_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
            time.sleep(0.5)
            date_input.send_keys(target_date + Keys.ENTER)
            
            print(f"📍 Jumped to {target_date} on {timeframe} chart.")
            time.sleep(12) # Wait for indicators to render

            # 5. UI Cleanup
            driver.execute_script("""
                document.querySelectorAll('[class*="overlap-"], [class*="modal-"], [class*="dialog-"], .tv-dialog__close').forEach(el => el.remove());
            """)
            ActionChains(driver).send_keys(Keys.ESCAPE).perform()
            time.sleep(1)
            
            # 6. Capture and Save
            img = driver.get_screenshot_as_png()
            if save_to_db(symbol, timeframe, img, target_date):
                print(f"✅ Saved {symbol} for {target_date} ({timeframe})")
            
        except Exception as e:
            print(f"❌ Error during {symbol} ({timeframe}): {str(e)[:100]}")
        finally:
            driver.quit()
        
        # Small delay between processing Week and Day for the same stock
        time.sleep(2)

def main():
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

if __name__ == "__main__":
    main()
