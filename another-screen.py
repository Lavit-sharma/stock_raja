import os, time, json, gspread
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
# Added for WebDriverWait and expected_conditions
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

def save_to_mysql(symbol, timeframe, image_data, chart_date):
    """Saves to DB with explicit commit."""
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
        conn.commit()
        print(f"    ∟ ✅ DB Updated: {symbol} ({timeframe}) on {chart_date}")
        
    except mysql.connector.Error as err:
        print(f"    ❌ DB Error: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def navigate_to_date(driver, date_str):
    """Robust Alt+G logic with focus and input checking."""
    if not date_str: return False
    try:
        # 1. Focus the chart first
        chart = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]"))
        )
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)

        # 2. Trigger Alt+G
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        # 3. Wait for the 'Go to' input box
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, input_xpath)))
        
        # 4. Clear and Type
        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(date_str)
        time.sleep(0.5)
        goto_input.send_keys(Keys.ENTER)
        
        time.sleep(5) 
        return True
    except Exception as e:
        print(f"    ⚠️ GoTo Dialog Failed: {e}")
        return False

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new") 
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    # Adds a real user agent to prevent being blocked
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    """Loads cookies from environment to bypass login screens."""
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
    except Exception as e:
        print(f"❌ Cookie Injection Error: {e}")
        return False

# ---------------- MAIN ---------------- #

def main():
    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        if not creds_json:
            print("❌ GSPREAD_CREDENTIALS not found.")
            return
            
        creds = json.loads(creds_json)
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        
        raw_data = worksheet.get_all_values()
        if not raw_data: return
        
        headers = [h.strip() if h.strip() else f"Col_{i}" for i, h in enumerate(raw_data[0])]
        df = pd.DataFrame(raw_data[1:], columns=headers)
    except Exception as e:
        print(f"❌ Initialization Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ TV Authentication Failed")
        driver.quit()
        return

    for _, row in df.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        day_url = str(row.get('Day', '')).strip()
        week_url = str(row.get('Week', '')).strip()
        
        # Calculate dates first to check for NaN/Null
        day_date = calculate_target_date(row.get('Days before'))
        week_date = calculate_target_date(row.get('Months before'))

        # SKIP LOGIC: If Symbol is empty OR either date is missing
        if not symbol or symbol.lower() == 'nan' or not day_date or not week_date:
            print(f"⏩ Skipping {symbol if symbol else 'Empty Row'}: Missing date or symbol data.")
            continue

        print(f"🚀 Processing {symbol}...")

        # Process both URLs
        tasks = [("day", day_url, day_date), ("week", week_url, week_date)]
        
        for timeframe, url, target_date in tasks:
            if "tradingview.com" not in url:
                continue
            try:
                driver.get(url)
                if navigate_to_date(driver, target_date):
                    # Take screenshot
                    chart_element = WebDriverWait(driver, 20).until(
                        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
                    )
                    img = chart_element.screenshot_as_png
                    save_to_mysql(symbol, timeframe, img, target_date)
            except Exception as e:
                print(f"    ⚠️ {timeframe} View Error for {symbol}: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
