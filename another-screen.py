import os, time, json, gspread
import pandas as pd
import numpy as np  # Added for NaN checking
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
SPREADSHEET_NAME = "Stock List"
TAB_NAME = "Weekday"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True
}

# ---------------- HELPERS ---------------- #

def calculate_target_date(input_val):
    """Calculates YYYY-MM-DD and handles null/NaN/empty values."""
    if pd.isna(input_val) or str(input_val).strip().lower() in ['nan', 'null', '']:
        return None
    try:
        # Extract digits (e.g., '104' from '104 before')
        digits = ''.join(filter(str.isdigit, str(input_val)))
        if not digits: return None
        
        days = int(digits)
        target_dt = datetime.now() - timedelta(days=days)
        return target_dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"    ⚠️ Date Calc Error: {e}")
        return None

def navigate_to_date(driver, date_str):
    """Robust Alt+G logic with focus and input checking."""
    if not date_str: return False
    try:
        # 1. Focus the chart first (essential for shortcuts to work)
        chart = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]"))
        )
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)

        # 2. Trigger Alt+G
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        # 3. Wait for the 'Go to' input box to appear
        # TV uses different classes, this covers the most common ones
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, input_xpath)))
        
        # 4. Type date and Enter
        goto_input.send_keys(Keys.CONTROL + "a") # Select existing text
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(date_str)
        time.sleep(0.5)
        goto_input.send_keys(Keys.ENTER)
        
        time.sleep(5) # Wait for chart to move
        return True
    except Exception as e:
        print(f"    ⚠️ GoTo Dialog Failed: {e}")
        return False

# ... [save_to_mysql, get_driver, inject_tv_cookies functions remain same as previous] ...

def main():
    # 1. Load Data
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        
        raw_data = worksheet.get_all_values()
        headers = [h.strip() if h.strip() else f"Col_{i}" for i, h in enumerate(raw_data[0])]
        df = pd.DataFrame(raw_data[1:], columns=headers)
    except Exception as e:
        print(f"❌ Initialization Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    for _, row in df.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        day_url = str(row.get('Day', '')).strip()
        week_url = str(row.get('Week', '')).strip()
        day_before_val = row.get('Days before')
        week_before_val = row.get('Months before')

        # --- VALIDATION: Skip if Symbol or Dates are NaN/Null ---
        day_date = calculate_target_date(day_before_val)
        week_date = calculate_target_date(week_before_val)

        if not symbol or symbol.lower() == 'nan' or not day_date or not week_date:
            print(f"⏩ Skipping {symbol if symbol else 'Empty Row'}: Missing date or symbol data.")
            continue

        print(f"🚀 Processing {symbol}...")

        # --- PROCESS DAY AND WEEK ---
        for timeframe, url, target_date in [("day", day_url, day_date), ("week", week_url, week_date)]:
            try:
                driver.get(url)
                if navigate_to_date(driver, target_date):
                    # Ensure chart is visible before screenshot
                    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                    img = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]").screenshot_as_png
                    save_to_mysql(symbol, timeframe, img, target_date)
            except Exception as e:
                print(f"    ⚠️ {timeframe} View Error: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
