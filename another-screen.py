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

def calculate_target_date(days_back_input):
    """Parses '4 before' or '1' into a YYYY-MM-DD string."""
    try:
        # Extract digits only (e.g., '4 before' -> 4)
        days = int(''.join(filter(str.isdigit, str(days_back_input))))
        target_dt = datetime.now() - timedelta(days=days)
        return target_dt.strftime('%Y-%m-%d')
    except Exception:
        return None

def navigate_to_date(driver, date_str):
    """Performs the Alt+G 'Go to Date' action in TradingView."""
    if not date_str: return False
    try:
        actions = ActionChains(driver)
        # Alt + G
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(1.5)
        # Type date and Enter
        actions.send_keys(date_str).send_keys(Keys.ENTER).perform()
        time.sleep(4) # Wait for chart to move
        return True
    except Exception as e:
        print(f"   ⚠️ Navigation Error: {e}")
        return False

def save_to_mysql(symbol, timeframe, image_data, chart_date):
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
        conn.commit()
    except Exception as e:
        print(f"   ❌ DB Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
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
            driver.add_cookie({"name": c.get("name"), "value": c.get("value"), "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

# ---------------- MAIN ---------------- #

def main():
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        sheet = client.open_by_url(STOCK_LIST_URL).sheet1
        data = sheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
    except Exception as e:
        print(f"❌ Spreadsheet Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ Auth Failed")
        driver.quit()
        return

    for _, row in df.iterrows():
        symbol = str(row.iloc[0]).strip()
        week_url = str(row.iloc[2]).strip()
        day_url = str(row.iloc[3]).strip()
        day_days_back = row.iloc[4]   # Column E: "1 before"
        week_days_back = row.iloc[5]  # Column F: "30 before"

        if not symbol or "tradingview.com" not in day_url:
            continue

        print(f"📸 Processing {symbol}...")

        # --- Process DAY (Day Before) ---
        try:
            driver.get(day_url)
            target_date = calculate_target_date(day_days_back)
            if target_date:
                navigate_to_date(driver, target_date)
            
            chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
            save_to_mysql(symbol, "day", chart.screenshot_as_png, target_date)
            print(f"   ∟ ✅ Saved Day Chart for {target_date}")
        except Exception as e: print(f"   ⚠️ Day Error: {e}")

        # --- Process WEEK (Month Before) ---
        try:
            driver.get(week_url)
            target_date = calculate_target_date(week_days_back)
            if target_date:
                navigate_to_date(driver, target_date)
                
            chart = WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
            save_to_mysql(symbol, "week", chart.screenshot_as_png, target_date)
            print(f"   ∟ ✅ Saved Week Chart for {target_date}")
        except Exception as e: print(f"   ⚠️ Week Error: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
