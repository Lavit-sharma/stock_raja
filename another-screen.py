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
        print(f"    ∟ ✅ Saved {symbol} ({timeframe})", flush=True)
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
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

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
        
        # 1. Open the "Weekday" tab
        sheet = spreadsheet.worksheet("Weekday") 
        data = sheet.get_all_values()
        
        # 2. Extract headers and clean them
        headers = [h.strip().lower() for h in data[0]]
        df = pd.DataFrame(data[1:], columns=headers)
        
        # --- DEBUGGER: See what column names Python actually sees ---
        print(f"DEBUG: Found headers in sheet: {headers}")
        
    except Exception as e:
        print(f"❌ Google Sheet Error: {e}")
        return

    driver = get_driver()
    # Note: Ensure cookies are injected here if needed

    for index, row in df.iterrows():
        # Using index 0 for Symbol and index 6 for Date as a fallback
        symbol = str(row.iloc[0]).strip()
        week_url = str(row.iloc[2]).strip()
        day_url = str(row.iloc[3]).strip()
        
        # FALLBACK LOGIC: Try to find 'dates here' column, otherwise take column 7 (index 6)
        target_date = row.get('dates here')
        if target_date is None or str(target_date).strip() == "":
            target_date = row.iloc[6] if len(row) > 6 else None

        # STRICT CLEANING
        target_date = str(target_date).strip()

        if target_date.lower() in ['nan', 'null', '', 'none', 'n/a']:
            print(f"⏭️ Skipping {symbol}: Read '{target_date}' from the dates column.")
            continue

        print(f"📸 Processing {symbol} for date: {target_date}...")

        # Process DAY
        try:
            if "tradingview.com" in day_url:
                driver.get(day_url)
                chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                navigate_to_date(driver, target_date)
                driver.execute_script("window.dispatchEvent(new Event('resize'));")
                time.sleep(2)
                save_to_mysql(symbol, "day", chart.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Day Error: {e}")

        # Process WEEK
        try:
            if "tradingview.com" in week_url:
                driver.get(week_url)
                chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                navigate_to_date(driver, target_date)
                driver.execute_script("window.dispatchEvent(new Event('resize'));")
                time.sleep(2)
                save_to_mysql(symbol, "week", chart.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Week Error: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
