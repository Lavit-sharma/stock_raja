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

# ---------------- HELPERS ---------------- #

def is_valid_date(date_val):
    """Returns True if the date is a non-empty, valid string."""
    if date_val is None:
        return False
    d_str = str(date_val).strip().lower()
    # Check for common 'empty' indicators
    if d_str in ["", "nan", "null", "none", "n/a"]:
        return False
    return True

def navigate_to_date(driver, date_str):
    """Triggers Alt+G, inputs the date, and presses Enter."""
    try:
        print(f"    ∟ 📅 Navigating to date: {date_str}")
        body = driver.find_element(By.TAG_NAME, "body")
        body.click()
        time.sleep(1)

        actions = ActionChains(driver)
        actions.key_down(Keys.ALT).send_keys('g').key_up(Keys.ALT).perform()
        time.sleep(2) 

        actions.send_keys(str(date_str)).send_keys(Keys.ENTER).perform()
        time.sleep(6) # Wait for chart to jump
        return True
    except Exception as e:
        print(f"    ∟ ⚠️ Date Navigation Error: {e}")
        return False

# ... [setup_database, save_to_mysql, get_driver, inject_tv_cookies remain same] ...

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
        
        # 1. GET THE DATE FROM COLUMN G
        try:
            target_date = row.iloc[6]
        except IndexError:
            target_date = None

        # 2. STRICT SKIP LOGIC
        if not is_valid_date(target_date):
            print(f"⏭️ Skipping {symbol}: No valid date found in Column G.")
            continue

        if not symbol or "tradingview.com" not in day_url:
            continue

        target_date = str(target_date).strip()
        print(f"📸 Processing {symbol} for date: {target_date}...")

        # --- Capture DAY ---
        try:
            driver.get(day_url)
            chart = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
            )
            
            navigate_to_date(driver, target_date)
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
