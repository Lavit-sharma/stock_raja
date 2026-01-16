import os, time, json, gspread
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
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

def calculate_target_date(days_back):
    """Calculates date string (YYYY-MM-DD) based on integer input."""
    try:
        # Extract only numbers from the sheet cell (e.g., '4 before' -> 4)
        days = int(''.join(filter(str.isdigit, str(days_back))))
        target_dt = datetime.now() - timedelta(days=days)
        return target_dt.strftime('%Y-%m-%d')
    except:
        return None

def navigate_to_date(driver, date_str):
    if not date_str: return False
    try:
        actions = ActionChains(driver)
        # Trigger Alt+G
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(1.5)
        # Type calculated date and Enter
        actions.send_keys(date_str).send_keys(Keys.ENTER).perform()
        time.sleep(4) 
        return True
    except Exception as e:
        print(f"   ⚠️ Navigation Error: {e}")
        return False

# ... [setup_database, save_to_mysql, get_driver, inject_tv_cookies stay the same] ...

def main():
    # setup_database() code here...
    
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        sheet = client.open_by_url(STOCK_LIST_URL).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        print(f"❌ GSht Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    for _, row in df.iterrows():
        symbol = str(row.iloc[0]).strip()
        week_url = str(row.iloc[2]).strip()
        day_url = str(row.iloc[3]).strip()
        
        # New: Get the "relative" numbers from columns 5 and 6
        day_days_back = row.iloc[4]   # e.g., "1" or "1 day before"
        month_days_back = row.iloc[5] # e.g., "30" or "1 month before"

        if not symbol or "tradingview.com" not in day_url:
            continue

        print(f"📸 Processing {symbol}...")

        # --- DAY CHART ---
        try:
            driver.get(day_url)
            target_day = calculate_target_date(day_days_back)
            if target_day:
                print(f"   ∟ Jumping to {target_day} ({day_days_back})")
                navigate_to_date(driver, target_day)
            
            chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
            save_to_mysql(symbol, "day", chart.screenshot_as_png)
        except Exception as e: print(f"⚠️ Day Error: {e}")

        # --- WEEK CHART ---
        try:
            driver.get(week_url)
            target_month = calculate_target_date(month_days_back)
            if target_month:
                print(f"   ∟ Jumping to {target_month} ({month_days_back})")
                navigate_to_date(driver, target_month)
            
            chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
            save_to_mysql(symbol, "week", chart.screenshot_as_png)
        except Exception as e: print(f"⚠️ Week Error: {e}")

    driver.quit()

if __name__ == "__main__":
    main()
