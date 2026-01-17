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
    "database": os.getenv("DB_NAME"),
    "autocommit": True
}

# ---------------- HELPERS ---------------- #

def calculate_target_date(input_val):
    """Calculates YYYY-MM-DD from '104 before' style strings."""
    try:
        if not input_val or str(input_val).strip() == "":
            return None
        digits = ''.join(filter(str.isdigit, str(input_val)))
        if not digits: return None
        
        days = int(digits)
        target_dt = datetime.now() - timedelta(days=days)
        return target_dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"   ⚠️ Date Calc Error: {e}")
        return None

def save_to_mysql(symbol, timeframe, image_data, chart_date):
    """Saves to DB with explicit commit to prevent empty tables."""
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
        
        cursor.execute(query, (symbol, timeframe, image_data, chart_date))
        conn.commit() # Save the data
        print(f"   ∟ ✅ DB Updated: {symbol} ({timeframe}) on {chart_date if chart_date else 'Current'}")
        
    except mysql.connector.Error as err:
        print(f"   ❌ DB Error: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def navigate_to_date(driver, date_str):
    """Triggers Alt+G and enters the calculated date."""
    if not date_str: return
    try:
        actions = ActionChains(driver)
        # Send Alt+G
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(2)
        # Type date and Enter
        actions.send_keys(date_str).send_keys(Keys.ENTER).perform()
        time.sleep(5) # Wait for chart to travel
    except Exception as e:
        print(f"   ⚠️ Alt+G Failed: {e}")

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
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
            driver.add_cookie({
                "name": c.get("name"), 
                "value": c.get("value"), 
                "domain": ".tradingview.com", 
                "path": "/"
            })
        driver.refresh()
        time.sleep(4)
        return True
    except: return False

# ---------------- MAIN ---------------- #

def main():
    # 1. Load and Clean Google Sheet Data
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        sheet = gc.open_by_url(STOCK_LIST_URL).sheet1
        
        # Avoid get_all_records() to prevent 'duplicate header' error
        raw_data = sheet.get_all_values()
        if not raw_data: return
        
        headers = [h.strip() if h.strip() else f"Col_{i}" for i, h in enumerate(raw_data[0])]
        df = pd.DataFrame(raw_data[1:], columns=headers)
    except Exception as e:
        print(f"❌ Sheet Loading Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ TV Authentication Failed")
        driver.quit()
        return

    for _, row in df.iterrows():
        # Match your exact structure
        symbol = str(row.get('Symbol', '')).strip()
        day_url = str(row.get('Day', '')).strip()
        week_url = str(row.get('Week', '')).strip()
        day_before_val = row.get('Days before')
        week_before_val = row.get('Months before')

        if not symbol or "tradingview.com" not in day_url:
            continue

        print(f"🚀 Processing {symbol}...")

        # --- DAY VIEW ---
        try:
            driver.get(day_url)
            target_date = calculate_target_date(day_before_val)
            if target_date:
                navigate_to_date(driver, target_date)
            
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
            img = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]").screenshot_as_png
            save_to_mysql(symbol, "day", img, target_date)
        except Exception as e:
            print(f"   ⚠️ Day View Error: {e}")

        # --- WEEK VIEW ---
        try:
            driver.get(week_url)
            target_date = calculate_target_date(week_before_val)
            if target_date:
                navigate_to_date(driver, target_date)
            
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
            img = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]").screenshot_as_png
            save_to_mysql(symbol, "week", img, target_date)
        except Exception as e:
            print(f"   ⚠️ Week View Error: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
