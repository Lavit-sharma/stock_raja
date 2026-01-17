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
        print("🧹 Cleaning database for new run...")
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
        query = "INSERT INTO another_screenshot (symbol, timeframe, screenshot, target_date) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (symbol, timeframe, image_data, target_date))
        conn.commit()
        print(f"    ∟ ✅ Saved {symbol} ({timeframe})")
    except Exception as e:
        print(f"    ∟ ❌ Save Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def navigate_to_date(driver, date_str):
    try:
        print(f"    ∟ 📅 Navigating to date: {date_str}")
        body = driver.find_element(By.TAG_NAME, "body")
        body.click()
        time.sleep(1)
        actions = ActionChains(driver)
        # Alt+G for TradingView GoTo
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
        sheet = spreadsheet.worksheet("Weekday") 
        data = sheet.get_all_values()
        
        # Header Row
        headers = data[0]
        # Find 'dates here' index (usually Column G)
        date_idx = next((i for i, h in enumerate(headers) if "dates here" in h.lower()), 6)
        
        df = pd.DataFrame(data[1:], columns=headers)
        print(f"✅ Found 'dates here' column at index {date_idx}")
    except Exception as e:
        print(f"❌ Google Sheet Error: {e}")
        return

    # Browser setup
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

    # --- Process Rows ---
    for index, row in df.iterrows():
        symbol = str(row.iloc[0]).strip() # Col A
        week_url = str(row.iloc[2]).strip() # Col C
        day_url = str(row.iloc[3]).strip() # Col D
        target_date = str(row.iloc[date_idx]).strip()

        # SKIP IF DATA IS MISSING
        if not symbol or target_date.lower() in ['', 'nan', 'null']:
            continue

        print(f"📸 Processing {symbol} for {target_date}...")

        # --- Capture DAY ---
        try:
            if "tradingview.com" in day_url:
                driver.get(day_url)
                WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                navigate_to_date(driver, target_date)
                chart = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")
                save_to_mysql(symbol, "day", chart.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Day Error: {e}")

        # --- Capture WEEK ---
        try:
            if "tradingview.com" in week_url:
                driver.get(week_url)
                WebDriverWait(driver, 30).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                navigate_to_date(driver, target_date)
                chart = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")
                save_to_mysql(symbol, "week", chart.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Week Error: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
