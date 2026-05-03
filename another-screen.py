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
    """Saves the binary image data to the MySQL database."""
    if not img_data or len(img_data) < 1000:
        print(f"⚠️ Skipping {symbol}: Image data empty or too small.")
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
        print(f"❌ DB Error for {symbol}: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

def process_row(row):
    """Handles the logic for a single stock row."""
    symbol = str(row.get("Symbol", "")).strip()
    url = str(row.get("Day", "")).strip()
    target_date = str(row.get("dates", "")).strip()

    if not symbol or "tradingview.com" not in url:
        return

    # UPDATED LOGGING: Now includes the URL and the target date
    print(f"🚀 Starting: {symbol} | Date: {target_date} | URL: {url}")
    driver = get_driver()
    
    try:
        # 1. Inject Authentication Cookies
        driver.get("https://www.tradingview.com/")
        cookies_env = os.getenv("TRADINGVIEW_COOKIES", "[]")
        cookies = json.loads(cookies_env)
        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c["name"], 
                    "value": c["value"], 
                    "domain": ".tradingview.com", 
                    "path": "/"
                })
            except: continue
        
        # 2. Navigate to Chart URL
        driver.get(url)
        wait = WebDriverWait(driver, 35)
        
        # 3. Locate Chart and Focus
        chart_xpath = "//div[contains(@class,'chart-container') or contains(@class,'chart-gui-wrapper')]"
        chart = wait.until(EC.presence_of_element_located((By.XPATH, chart_xpath)))
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(2)
        
        # 4. Trigger "Go To Date" (Alt + G)
        ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()
        
        # 5. Input Target Date
        input_xpath = "//input[contains(@class,'query') or contains(@class,'input')]"
        date_input = wait.until(EC.element_to_be_clickable((By.XPATH, input_xpath)))
        date_input.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        time.sleep(0.5)
        date_input.send_keys(target_date + Keys.ENTER)
        
        # UPDATED LOGGING: Confirming the GoTo action
        print(f"📍 Executed 'Go To' for: {target_date}")
        
        # 6. Wait for technical indicators to render
        print(f"⏳ Rendering {symbol}...")
        time.sleep(12) 

        # --- UPDATED: AGGRESSIVE POPUP REMOVAL ---
        driver.execute_script("""
            const selectors = [
                '[class*="overlap-"]', 
                '[class*="modal-"]', 
                '[class*="dialog-"]', 
                '.tv-dialog__close', 
                '.js-dialog__close'
            ];
            selectors.forEach(selector => {
                document.querySelectorAll(selector).forEach(el => el.remove());
            });
        """)
        
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(0.5)
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(1)
        
        # 7. Capture Screenshot and Save
        img = driver.get_screenshot_as_png()
        if save_to_db(symbol, "day", img, target_date):
            print(f"✅ {symbol} processed successfully.")
        
    except Exception as e:
        print(f"❌ Error during {symbol}: {str(e)[:100]}")
    finally:
        driver.quit()

def main():
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json:
            print("❌ GSPREAD_CREDENTIALS env var is missing.")
            return
            
        creds = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds)
        sh = gc.open("Stock List").worksheet("Weekday")
        rows = sh.get_all_records()
    except Exception as e:
        print(f"❌ Failed to load Google Sheet: {e}")
        return

    start = int(os.getenv("START_ROW", 0))
    end = int(os.getenv("END_ROW", 500))
    selected_rows = rows[start:end]
    
    print(f"📦 Total rows to process: {len(selected_rows)} (Range: {start}-{end})")

    for row in selected_rows:
        process_row(row)
        time.sleep(1)

if __name__ == "__main__":
    main()
