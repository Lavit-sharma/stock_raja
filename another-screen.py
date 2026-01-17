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

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME")
}

# ---------------- HELPERS ---------------- #

def setup_database():
    """Updated to include a date column for better tracking."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Added 'target_date' column and updated the UNIQUE constraint
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
        
        print("🧹 Note: Clearing old entries...", flush=True)
        cursor.execute("TRUNCATE TABLE another_screenshot")
        conn.commit()
        print("✅ Database setup and cleaned.", flush=True)
    except Exception as e:
        print(f"❌ Database Setup Error: {e}", flush=True)
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def save_to_mysql(symbol, timeframe, image_data, target_date):
    """Saves screenshot with the specific date reference."""
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
        print(f"    ∟ ✅ Saved {symbol} ({timeframe}) for date {target_date}", flush=True)
    except Exception as e:
        print(f"    ∟ ❌ Save Error: {e}", flush=True)
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def navigate_to_date(driver, date_str):
    """Triggers Alt+G, inputs the date, and presses Enter."""
    try:
        if not date_str or str(date_str).strip() == "" or str(date_str).lower() == "nan":
            return False
            
        print(f"    ∟ 📅 Navigating to date: {date_str}")
        
        # Ensure the chart has focus
        body = driver.find_element(By.TAG_NAME, "body")
        body.click()
        time.sleep(1)

        actions = ActionChains(driver)
        # Alt + G opens the 'Go To' dialog
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(2) 

        # Type the date and hit Enter
        actions.send_keys(str(date_str)).send_keys(Keys.ENTER).perform()
        
        # Wait for chart to jump and indicators to reload
        time.sleep(6)
        return True
    except Exception as e:
        print(f"    ∟ ⚠️ Date Navigation Error: {e}")
        return False

# ... [get_driver and inject_tv_cookies functions remain the same as your snippet] ...

def main():
    setup_database()

    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        sheet = client.open_by_url(STOCK_LIST_URL).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        print(f"❌ Google Sheet Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ TradingView Authentication Failed")
        driver.quit()
        return

    for _, row in df.iterrows():
        symbol = str(row.iloc[0]).strip()
        week_url = str(row.iloc[2]).strip()
        day_url = str(row.iloc[3]).strip()
        
        # Date is in Column G (index 6)
        try:
            target_date = str(row.iloc[6]).strip()
        except:
            target_date = "No Date"

        if not symbol or "tradingview.com" not in day_url:
            continue

        print(f"📸 Processing {symbol} for date: {target_date}...")

        # --- Capture DAY ---
        try:
            driver.get(day_url)
            chart = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
            )
            
            navigate_to_date(driver, target_date)
            
            # Force UI refresh for indicators
            driver.execute_script("window.dispatchEvent(new Event('resize'));")
            time.sleep(2)
            
            save_to_mysql(symbol, "day", chart.screenshot_as_png, target_date)
        except Exception as e:
            print(f"    ⚠️ Day Error: {e}")

        # --- Capture WEEK ---
        try:
            driver.get(week_url)
            chart = WebDriverWait(driver, 25).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
            )
            
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
