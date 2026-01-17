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
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=1400370843"

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
                target_date VARCHAR(50),
                screenshot LONGBLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY symbol_tf_date (symbol, timeframe, target_date)
            ) ENGINE=InnoDB;
        """)
        print("🧹 Cleaning database for new run...", flush=True)
        cursor.execute("TRUNCATE TABLE another_screenshot")
        conn.commit()
    except Exception as e:
        print(f"❌ DB Setup Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def save_to_mysql(symbol, timeframe, image_data, target_date):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, target_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, target_date))
        conn.commit()
        print(f"    ∟ ✅ Saved {symbol} ({timeframe}) to Database", flush=True)
    except Exception as e:
        print(f"    ∟ ❌ Save Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--force-device-scale-factor=1")
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
            try:
                driver.add_cookie({
                    "name": c.get("name"), 
                    "value": c.get("value"), 
                    "domain": ".tradingview.com", 
                    "path": "/"
                })
            except: pass
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

def navigate_to_date(driver, date_str):
    try:
        print(f"    ∟ 📅 Navigating to date: {date_str}")
        body = driver.find_element(By.TAG_NAME, "body")
        body.click()
        time.sleep(1)
        actions = ActionChains(driver)
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(2) 
        actions.send_keys(str(date_str)).send_keys(Keys.ENTER).perform()
        time.sleep(6) 
        return True
    except Exception as e:
        print(f"    ∟ ⚠️ Navigation Error: {e}")
        return False

# ---------------- MAIN ---------------- #

def main():
    setup_database()

    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        spreadsheet = client.open_by_url(STOCK_LIST_URL)
        
        # Open exactly the "Weekday" tab
        sheet = spreadsheet.worksheet("Weekday") 
        
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        
        # Clean column names
        df.columns = [c.strip().lower() for c in df.columns]
        print(f"✅ Connected to 'Weekday' tab. Ready to process {len(df)} symbols.")
        
    except Exception as e:
        print(f"❌ Google Sheet Error: {e}. Ensure tab 'Weekday' exists!")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ TradingView Authentication Failed")
        driver.quit()
        return

    for _, row in df.iterrows():
        # Match your exact header names (mapping to lowercase clean versions)
        symbol = str(row.get('symbol', '')).strip()
        week_url = str(row.get('week', '')).strip()
        day_url = str(row.get('day', '')).strip()
        target_date = str(row.get('dates here', '')).strip()

        # SKIP LOGIC: Skip if no date or if the cell is empty/NaN
        if not symbol or target_date.lower() in ['nan', 'null', '', 'none']:
            print(f"⏭️ Skipping {symbol}: No valid date in 'dates here'.")
            continue

        print(f"📸 Processing {symbol}...")

        # --- Capture DAY ---
        try:
            if "tradingview.com" in day_url:
                driver.get(day_url)
                chart_element = WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
                )
                navigate_to_date(driver, target_date)
                driver.execute_script("window.dispatchEvent(new Event('resize'));")
                time.sleep(2)
                save_to_mysql(symbol, "day", chart_element.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Day Error for {symbol}: {e}")

        # --- Capture WEEK ---
        try:
            if "tradingview.com" in week_url:
                driver.get(week_url)
                chart_element = WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
                )
                navigate_to_date(driver, target_date)
                driver.execute_script("window.dispatchEvent(new Event('resize'));")
                time.sleep(2)
                save_to_mysql(symbol, "week", chart_element.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Week Error for {symbol}: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
