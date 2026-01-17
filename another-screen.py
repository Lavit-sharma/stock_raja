import os, time, json, gspread
import pandas as pd
import mysql.connector
from datetime import datetime
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

def get_valid_date(input_val):
    """Returns the date string if valid, otherwise returns None to skip."""
    if pd.isna(input_val) or str(input_val).strip().lower() in ['nan', 'null', '']:
        return None
    return str(input_val).strip()

def save_to_mysql(symbol, timeframe, image_data, chart_date):
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
    """Clicks chart and uses Alt+G to enter exact date string."""
    try:
        # 1. Focus the chart
        chart = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]"))
        )
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)

        # 2. Trigger Alt+G
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        # 3. Locate the 'Go to' input field
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = WebDriverWait(driver, 7).until(EC.presence_of_element_located((By.XPATH, input_xpath)))
        
        # 4. Clear and type the exact date from Excel
        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(date_str)
        time.sleep(0.5)
        goto_input.send_keys(Keys.ENTER)
        
        # Wait for the chart to jump to the date
        time.sleep(6) 
        return True
    except Exception as e:
        print(f"    ⚠️ Could not navigate to {date_str}: {e}")
        return False

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new") 
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
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
        time.sleep(4)
        return True
    except: return False

# ---------------- MAIN ---------------- #

def main():
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        
        # Using get_all_records to easily map column names
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)
    except Exception as e:
        print(f"❌ Spreadsheet Loading Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        print("❌ TradingView Cookie Injection Failed.")
        driver.quit()
        return

    for index, row in df.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        
        # Get dates directly from columns without calculation
        day_date = get_valid_date(row.get('Days before'))
        week_date = get_valid_date(row.get('Months before'))

        # Skip if symbol or dates are missing
        if not symbol or symbol.lower() == 'nan' or not day_date or not week_date:
            print(f"⏩ Skipping Row {index+2} ({symbol}): Missing date or symbol.")
            continue

        print(f"🚀 Processing {symbol}...")

        # Timeframes to process
        chart_tasks = [
            ("day", str(row.get('Day', '')), day_date),
            ("week", str(row.get('Week', '')), week_date)
        ]

        for timeframe, url, target_date in chart_tasks:
            if "tradingview.com" not in url:
                continue
                
            try:
                driver.get(url)
                # Wait for initial load
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                
                if navigate_to_date(driver, target_date):
                    # Final check for chart visibility then screenshot
                    chart_elem = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")
                    img = chart_elem.screenshot_as_png
                    save_to_mysql(symbol, timeframe, img, target_date)
            except Exception as e:
                print(f"    ⚠️ Error on {timeframe} view for {symbol}: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
