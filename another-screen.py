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

def save_to_mysql(symbol, timeframe, image_data, chart_date):
    """Saves the screenshot to MySQL with the provided date string."""
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
    """Focuses the chart, triggers Alt+G, and enters the exact date string."""
    try:
        # Focus chart area
        chart = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]"))
        )
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)

        # Open Go-to dialog
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        # Enter the date
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, input_xpath)))
        
        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        goto_input.send_keys(str(date_str))
        time.sleep(0.5)
        goto_input.send_keys(Keys.ENTER)
        
        time.sleep(7) # Wait for chart jump
        return True
    except Exception as e:
        print(f"    ⚠️ Navigation failed for {date_str}: {e}")
        return False

def get_driver():
    """Initializes the Chrome WebDriver."""
    opts = Options()
    opts.add_argument("--headless=new") 
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=opts)

def inject_tv_cookies(driver):
    """Bypasses login using TradingView cookies."""
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(5)
        for c in cookies:
            driver.add_cookie({"name": c.get("name"), "value": c.get("value"), "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

# ---------------- MAIN ---------------- #

def main():
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME) # Open Stock List
        worksheet = spreadsheet.worksheet(TAB_NAME) # Open Weekday tab
        
        all_values = worksheet.get_all_values()
        if not all_values: return

        # Clean headers and ensure 'dates' column is found
        headers = [h.strip() for h in all_values[0] if h.strip()]
        df = pd.DataFrame([row[:len(headers)] for row in all_values[1:]], columns=headers)
        
    except Exception as e:
        print(f"❌ Sheet Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    for index, row in df.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        week_url = str(row.get('Week', '')).strip()
        day_url = str(row.get('Day', '')).strip()
        
        # Pick the direct date from the "dates" column (Column G)
        target_date = str(row.get('dates', '')).strip()

        # Skip logic: If symbol or date is missing, move to next
        if not symbol or symbol.lower() in ['nan', ''] or not target_date or target_date.lower() in ['nan', '']:
            if symbol or day_url:
                print(f"⏩ Skipping Row {index+2} ({symbol}): Missing 'dates' or symbol.")
            continue

        print(f"🚀 Processing {symbol} using date: {target_date}...")

        # Process Day View and Week View using the SAME date from Column G
        for timeframe, url in [("day", day_url), ("week", week_url)]:
            if "tradingview.com" not in str(url):
                continue
            try:
                driver.get(url)
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                
                if navigate_to_date(driver, target_date):
                    chart_elem = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")
                    img = chart_elem.screenshot_as_png
                    save_to_mysql(symbol, timeframe, img, target_date)
            except Exception as e:
                print(f"    ⚠️ Error on {timeframe} for {symbol}: {e}")

    driver.quit()
    print("🏁 PROCESS COMPLETE!")

if __name__ == "__main__":
    main()
