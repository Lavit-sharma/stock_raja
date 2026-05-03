import os
import time
import json
import gspread
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

# --- MANDATORY CONFIG ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306"))
}

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

def save_to_db(symbol, timeframe, img_data, chart_date):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = "INSERT INTO another_screenshot (symbol, timeframe, screenshot, chart_date) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (symbol, timeframe, img_data, chart_date))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"DB Error for {symbol}: {e}")

def process_symbol(row):
    symbol = str(row.get("Symbol", "")).strip()
    url = str(row.get("Day", "")).strip()
    target_date = str(row.get("dates", "")).strip()

    if not symbol or not url: return

    driver = get_driver()
    try:
        # 1. Login via Cookies
        driver.get("https://www.tradingview.com/")
        cookies = json.loads(os.getenv("TRADINGVIEW_COOKIES"))
        for c in cookies:
            driver.add_cookie({"name": c["name"], "value": c["value"], "domain": ".tradingview.com", "path": "/"})
        
        # 2. Go to Chart
        driver.get(url)
        wait = WebDriverWait(driver, 30)
        chart = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "chart-container")))
        
        # 3. Go to Date (Alt+G)
        ActionChains(driver).move_to_element(chart).click().perform()
        time.sleep(1)
        ActionChains(driver).key_down(Keys.ALT).send_keys("g").key_up(Keys.ALT).perform()
        
        input_box = wait.until(EC.element_to_be_clickable((By.XPATH, "//input[contains(@class,'input')]")))
        input_box.send_keys(Keys.CONTROL + "a" + Keys.BACKSPACE)
        input_box.send_keys(target_date + Keys.ENTER)
        
        # 4. Wait & Snap
        print(f"Processing {symbol} for date {target_date}...")
        time.sleep(10) 
        
        img = driver.get_screenshot_as_png()
        save_to_db(symbol, "day", img, target_date)
        print(f"✅ {symbol} saved.")

    except Exception as e:
        print(f"❌ Error {symbol}: {e}")
    finally:
        driver.quit()

def main():
    # Load Sheet
    creds = json.loads(os.getenv("GSPREAD_CREDENTIALS"))
    gc = gspread.service_account_from_dict(creds)
    sh = gc.open("Stock List").worksheet("Weekday")
    rows = sh.get_all_records()
    
    # Process range
    start = int(os.getenv("START_ROW", 0))
    end = int(os.getenv("END_ROW", 500))
    
    for row in rows[start:end]:
        process_row(row)

if __name__ == "__main__":
    main()
