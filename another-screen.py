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
    """Saves screenshot. Ensures date is NULL if empty to avoid 0000-00-00."""
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # If date is invalid, set to None so DB stores it as NULL or current date
        final_date = chart_date if chart_date and chart_date != "" else None
        
        query = """
            INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                chart_date = VALUES(chart_date),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data, final_date))
        conn.commit()
        print(f"    ∟ ✅ DB Updated: {symbol} ({timeframe}) at {final_date}")
    except mysql.connector.Error as err:
        print(f"    ❌ DB Error: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def navigate_to_date(driver, date_str):
    """Opens Alt+G, types date, and WAITS for chart movement."""
    try:
        # 1. Force Focus on Chart
        chart = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'chart-container')]"))
        )
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)

        # 2. Trigger Alt+G
        ActionChains(driver).key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        
        # 3. Locate Input and Clear it thoroughly
        input_xpath = "//input[contains(@class, 'query') or @data-role='search' or contains(@class, 'input')]"
        goto_input = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, input_xpath)))
        
        goto_input.send_keys(Keys.CONTROL + "a")
        goto_input.send_keys(Keys.BACKSPACE)
        time.sleep(0.5)
        
        # 4. Type date and hit Enter
        goto_input.send_keys(str(date_str))
        time.sleep(1)
        goto_input.send_keys(Keys.ENTER)
        
        # 5. CRITICAL: Wait for chart to 'jump'
        # We wait 8 seconds because historical data takes time to load
        print(f"    ∟ ⏳ Traveling to {date_str}...")
        time.sleep(8) 
        return True
    except Exception as e:
        print(f"    ⚠️ Navigation Error: {e}")
        return False

# ... [get_driver and inject_tv_cookies functions remain unchanged] ...

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
        time.sleep(5)
        for c in cookies:
            driver.add_cookie({"name": c.get("name"), "value": c.get("value"), "domain": ".tradingview.com", "path": "/"})
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

def main():
    try:
        creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        spreadsheet = gc.open(SPREADSHEET_NAME)
        worksheet = spreadsheet.worksheet(TAB_NAME)
        all_values = worksheet.get_all_values()
        
        headers = [h.strip() for h in all_values[0]]
        df = pd.DataFrame(all_values[1:], columns=headers)
    except Exception as e:
        print(f"❌ Spreadsheet Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    for index, row in df.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        week_url = str(row.get('Week', '')).strip()
        day_url = str(row.get('Day', '')).strip()
        target_date = str(row.get('dates', '')).strip()

        if not symbol or not target_date or target_date.lower() in ['nan', '']:
            continue

        print(f"🚀 {symbol} | Target: {target_date}")

        for timeframe, url in [("day", day_url), ("week", week_url)]:
            if "tradingview.com" not in str(url): continue
            try:
                driver.get(url)
                # Ensure chart is loaded
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]")))
                
                if navigate_to_date(driver, target_date):
                    # Take screenshot after the 8-second wait in navigate_to_date
                    chart_elem = driver.find_element(By.XPATH, "//div[contains(@class, 'chart-container')]")
                    img = chart_elem.screenshot_as_png
                    save_to_mysql(symbol, timeframe, img, target_date)
            except Exception as e:
                print(f"    ⚠️ Failed {timeframe} for {symbol}: {e}")

    driver.quit()

if __name__ == "__main__":
    main()
