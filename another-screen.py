import os, time, json, gspread
import pandas as pd
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

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"

# Change these strings to match your EXACT Google Sheet column header names
COL_SYMBOL = "Symbol"
COL_WEEK_URL = "Week URL"
COL_DAY_URL = "Day URL"
COL_TARGET_DATE = "Target Date" 

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# ---------------- HELPERS ---------------- #

def setup_database():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS another_screenshot (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                timeframe VARCHAR(20) NOT NULL,
                screenshot LONGBLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY symbol_tf (symbol, timeframe)
            ) ENGINE=InnoDB;
        """)
        print("🧹 Clearing old entries...", flush=True)
        cursor.execute("TRUNCATE TABLE another_screenshot")
        conn.commit()
        print("✅ Database cleaned.", flush=True)
    except Exception as e:
        print(f"❌ Database Setup Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close(); conn.close()

def save_to_mysql(symbol, timeframe, image_data):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE screenshot = VALUES(screenshot), created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data))
        conn.commit()
        print(f"   ∟ ✅ Saved {symbol} ({timeframe})")
    except Exception as e:
        print(f"   ∟ ❌ Save Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close(); conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Force ignore cache to ensure fresh URL loading
    opts.add_argument("--disable-cache")
    opts.add_argument("--disk-cache-size=0")
    opts.add_argument("--force-device-scale-factor=1")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        for c in cookies:
            driver.add_cookie({"name": c.get("name"), "value": c.get("value"), "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

def navigate_to_date(driver, date_str):
    try:
        if not date_str or str(date_str).strip() == "": return False
        print(f"   ∟ 📅 Navigating to date: {date_str}")
        body = driver.find_element(By.TAG_NAME, "body")
        body.click()
        time.sleep(1)
        actions = ActionChains(driver)
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(2)
        actions.send_keys(str(date_str)).send_keys(Keys.ENTER).perform()
        time.sleep(6) # Increased wait for data loading
        return True
    except Exception as e:
        print(f"   ∟ ⚠️ Date Error: {e}"); return False

# ---------------- MAIN ---------------- #

def main():
    setup_database()

    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        sheet = client.open_by_url(STOCK_LIST_URL).sheet1
        
        # Fetch data and convert to DataFrame using the first row as headers
        data = sheet.get_all_records() 
        df = pd.DataFrame(data)
        print(f"📋 Loaded {len(df)} symbols from Google Sheets.")
    except Exception as e:
        print(f"❌ Google Sheet Error: {e}"); return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ TV Auth Failed"); driver.quit(); return

    for index, row in df.iterrows():
        # DYNAMIC COLUMN MAPPING: This finds the data by name, not number
        symbol = str(row.get(COL_SYMBOL, "")).strip()
        week_url = str(row.get(COL_WEEK_URL, "")).strip()
        day_url = str(row.get(COL_DAY_URL, "")).strip()
        target_date = str(row.get(COL_TARGET_DATE, "")).strip()

        if not symbol or "tradingview.com" not in day_url:
            print(f"⏩ Skipping Row {index+1}: Missing URL or Symbol")
            continue

        print(f"📸 [{index+1}/{len(df)}] Processing {symbol}...")

        # Process Day URL
        try:
            print(f"   ∟ Opening Day URL: {day_url}")
            driver.get(day_url)
            chart = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
            )
            if target_date: navigate_to_date(driver, target_date)
            
            # Recalculate and Wiggle
            driver.execute_script("window.dispatchEvent(new Event('resize'));")
            time.sleep(5) 
            ActionChains(driver).move_by_offset(10, 10).perform()
            
            save_to_mysql(symbol, "day", chart.screenshot_as_png)
        except Exception as e:
            print(f"   ⚠️ Day Error for {symbol}: {e}")

        # Process Week URL
        try:
            print(f"   ∟ Opening Week URL: {week_url}")
            driver.get(week_url)
            chart = WebDriverWait(driver, 25).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
            )
            if target_date: navigate_to_date(driver, target_date)
            
            driver.execute_script("window.dispatchEvent(new Event('resize'));")
            time.sleep(3)
            save_to_mysql(symbol, "week", chart.screenshot_as_png)
        except Exception as e:
            print(f"   ⚠️ Week Error for {symbol}: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
