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
MV2_SQL_URL    = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

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
        print("🧹 Clearing old database entries...", flush=True)
        cursor.execute("TRUNCATE TABLE stock_screenshots")
        conn.commit()
        print("✅ Database is clean.", flush=True)
    except Exception as e:
        print(f"❌ Error clearing database: {e}", flush=True)
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
        print(f"✅ [DB] Updated/Saved {symbol} ({timeframe})", flush=True)
    except Exception as e:
        print(f"❌ Database Error: {e}", flush=True)
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
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def inject_tv_cookies(driver):
    try:
        cookie_data = os.getenv("TRADINGVIEW_COOKIES")
        if not cookie_data:
            return False
        cookies = json.loads(cookie_data)
        driver.get("https://www.tradingview.com/")
        time.sleep(3)
        for c in cookies:
            try:
                driver.add_cookie({
                    "name": c.get("name"),
                    "value": c.get("value"),
                    "domain": c.get("domain", ".tradingview.com"),
                    "path": c.get("path", "/")
                })
            except:
                pass
        driver.refresh()
        time.sleep(5)
        return True
    except:
        return False

# ---------------- MAIN ---------------- #

def main():
    clear_db_before_run()

    try:
        creds_json = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds_json))

        mv2_raw = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_raw[1:], columns=mv2_raw[0])

        stock_raw = client.open_by_url(STOCK_LIST_URL).sheet1.get_all_values()
        df_stocks = pd.DataFrame(stock_raw[1:], columns=stock_raw[0])

        link_map = dict(zip(
            df_stocks.iloc[:, 0].astype(str).str.strip(),
            df_stocks.iloc[:, 2].astype(str).str.strip()
        ))
    except Exception as e:
        print(f"❌ Sheet Error: {e}")
        return

    driver = get_driver()
    if not inject_tv_cookies(driver):
        driver.quit()
        return

    for _, row in df_mv2.iterrows():
        symbol = str(row.get('Symbol', '')).strip()

        # ✅ SECTOR REJECTION (NEW)
        sector = str(row.get('Sector', '')).strip().upper()
        if sector in ("INDICES", "MUTUAL FUND SCHEME"):
            continue

        try:
            daily = float(str(row.get('dailychange', '0')).replace('%', '').strip() or 0)
            monthly = float(str(row.get('monthlychange', '0')).replace('%', '').strip() or 0)
        except:
            continue

        if daily >= 0.07 or monthly >= 0.25:
            url = link_map.get(symbol)
            if not url or "tradingview.com" not in url:
                continue

            driver.get(url)
            try:
                chart = WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located(
                        (By.XPATH, "//div[contains(@class, 'chart-container')]")
                    )
                )
                time.sleep(8)

                if daily >= 0.07:
                    webdriver.ActionChains(driver).send_keys("1D").send_keys(Keys.ENTER).perform()
                    time.sleep(5)
                    save_to_mysql(symbol, "daily", chart.screenshot_as_png)

                if monthly >= 0.25:
                    webdriver.ActionChains(driver).send_keys("1M").send_keys(Keys.ENTER).perform()
                    time.sleep(5)
                    save_to_mysql(symbol, "monthly", chart.screenshot_as_png)

            except Exception as e:
                print(f"⚠️ Screenshot Error ({symbol}): {e}")

    driver.quit()
    print("🏁 DONE!")

if __name__ == "__main__":
    main()
