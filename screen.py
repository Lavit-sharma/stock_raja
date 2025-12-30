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

def clear_db_before_run():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("🧹 Clearing old database screenshots...", flush=True)
        cursor.execute("TRUNCATE TABLE stock_screenshots")
        conn.commit()
    except Exception as e:
        print(f"❌ Error clearing database: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_filtered_alerts_from_db():
    """Fetches alerts and fixes the Pandas UserWarning by using a dictionary cursor."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT Symbol, dailychange, monthlychange 
            FROM wp_mv2 
            WHERE (CAST(REPLACE(dailychange, '%', '') AS DECIMAL(10,4)) >= 0.07)
               OR (CAST(REPLACE(monthlychange, '%', '') AS DECIMAL(10,4)) >= 0.25)
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"📊 [DB] SQL Filtered: {len(rows)} symbols matching conditions.", flush=True)
        return pd.DataFrame(rows) 
    except Exception as e:
        print(f"❌ Error fetching alerts: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def save_to_mysql(symbol, timeframe, image_data):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = """
            INSERT INTO stock_screenshots (symbol, timeframe, screenshot) 
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                screenshot = VALUES(screenshot),
                created_at = CURRENT_TIMESTAMP
        """
        cursor.execute(query, (symbol, timeframe, image_data))
        conn.commit()
        print(f"✅ [DB] Saved: {symbol} ({timeframe})", flush=True)
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def inject_tv_cookies(driver):
    print("🔑 Injecting TradingView Session...", flush=True)
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data: return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"), "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"), "path": c.get("path", "/")
                })
            except: pass
        driver.refresh()
        time.sleep(5)
        return True
    except: return False

# ---------------- MAIN ---------------- #

def main():
    clear_db_before_run()

    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))
        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])
        link_map = dict(zip(df_stocks.iloc[:, 0].astype(str).str.strip(), 
                            df_stocks.iloc[:, 2].astype(str).str.strip()))

        df_mv2 = get_filtered_alerts_from_db()
        
        if df_mv2.empty:
            print("🏁 No symbols hit criteria today. DONE!")
            return
    except Exception as e:
        print(f"❌ Initialization Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    for _, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        
        # FIX: Added .replace('−', '-') to handle Unicode minus signs from TradingView
        try:
            daily_val = float(str(row.get('dailychange', '0')).replace('−', '-').replace('%', '').strip() or 0)
            monthly_val = float(str(row.get('monthlychange', '0')).replace('−', '-').replace('%', '').strip() or 0)
        except ValueError:
            print(f"⚠️ Could not parse values for {symbol}, skipping...")
            continue

        url = link_map.get(symbol)
        if not url or "tradingview.com" not in url: 
            continue

        print(f"🚀 Processing: {symbol}...")
        driver.get(url)
        try:
            chart = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'chart-container')]"))
            )
            time.sleep(8) 

            if daily_val >= 0.07:
                webdriver.ActionChains(driver).send_keys("1D").send_keys(Keys.ENTER).perform()
                time.sleep(5)
                save_to_mysql(symbol, "daily", chart.screenshot_as_png)

            if monthly_val >= 0.25:
                webdriver.ActionChains(driver).send_keys("1M").send_keys(Keys.ENTER).perform()
                time.sleep(5)
                save_to_mysql(symbol, "monthly", chart.screenshot_as_png)
                    
        except Exception as e:
            print(f"⚠️ Screenshot Error ({symbol}): {e}")

    driver.quit()
    print("🏁 ALL TASKS COMPLETED!")

if __name__ == "__main__":
    main()
