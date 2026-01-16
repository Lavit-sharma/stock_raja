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

# ---------------- HELPERS ---------------- #

def calculate_target_date(input_val):
    """
    Parses '104 days before' or '4' into a YYYY-MM-DD string.
    If parsing fails, returns None.
    """
    try:
        if not input_val or str(input_val).strip() == "":
            return None
        
        # Extract only digits from the string
        digits = ''.join(filter(str.isdigit, str(input_val)))
        if not digits:
            return None
            
        days = int(digits)
        # Calculate back from TODAY (Jan 16, 2026)
        target_dt = datetime.now() - timedelta(days=days)
        return target_dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"   ⚠️ Date Calc Error: {e}")
        return None

def save_to_mysql(symbol, timeframe, image_data, chart_date):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        # Use COALESCE or handle NULL explicitly to ensure the DB accepts it
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, chart_date))
        conn.commit()
    except Exception as e:
        print(f"   ❌ DB Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

# ... (keep get_driver, inject_tv_cookies, navigate_to_date same as before)

def main():
    # ... (setup auth)
    
    # Reading by records to handle column headers properly
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    for _, row in df.iterrows():
        # Using exact names from your structure
        symbol = str(row.get('Symbol', '')).strip()
        day_url = str(row.get('Day', '')).strip()
        week_url = str(row.get('Week', '')).strip()
        day_before_val = row.get('Days before', None)
        month_before_val = row.get('Months before', None)

        if not symbol or "tradingview.com" not in day_url:
            continue

        print(f"📸 Processing {symbol}...")

        # --- DAY CHART ---
        target_day = calculate_target_date(day_before_val)
        driver.get(day_url)
        if target_day:
            print(f"   ∟ Jumping to Day: {target_day}")
            navigate_to_date(driver, target_day)
        
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
        save_to_mysql(symbol, "day", driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]").screenshot_as_png, target_day)

        # --- WEEK CHART ---
        target_week = calculate_target_date(month_before_val)
        driver.get(week_url)
        if target_week:
            print(f"   ∟ Jumping to Week: {target_week}")
            navigate_to_date(driver, target_week)
            
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
        save_to_mysql(symbol, "week", driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]").screenshot_as_png, target_week)

    driver.quit()
