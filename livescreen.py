import os
import time
import mysql.connector

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

SOURCE_TABLE = "wp_live_close"
TARGET_TABLE = "live_screen"

CHART_WAIT_SEC = 25
POST_LOAD_SLEEP = 5

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- DB ---------------- #
class DB:
    def __init__(self, config):
        self.conn = mysql.connector.connect(**config)
        self.conn.autocommit = True

    def fetch_symbols(self):
        query = f"""
            SELECT Symbol, real_close, real_change
            FROM {SOURCE_TABLE}
            WHERE CAST(real_change AS DECIMAL(10,2)) >= 7
        """
        cur = self.conn.cursor(dictionary=True)
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return rows

    def save(self, symbol, real_close, real_change, image):
        query = f"""
            INSERT INTO {TARGET_TABLE} 
            (symbol, real_close, real_change, screenshot)
            VALUES (%s, %s, %s, %s)
        """
        cur = self.conn.cursor()
        cur.execute(query, (symbol, real_close, real_change, image))
        cur.close()

    def close(self):
        self.conn.close()


# ---------------- SELENIUM ---------------- #
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )


# ---------------- MAIN LOGIC ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = get_driver()

    try:
        rows = db.fetch_symbols()

        print(f"🔥 Found {len(rows)} stocks with change >= 7")

        for row in rows:
            symbol = row["Symbol"]
            real_close = row["real_close"]
            real_change = float(row["real_change"])

            print(f"🚀 Processing: {symbol} ({real_change}%)")

            # 👉 TradingView URL (EDIT if needed)
            url = f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}"

            try:
                driver.get(url)

                chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                    EC.presence_of_element_located(
                        (By.XPATH, "//div[contains(@class,'chart-container')]")
                    )
                )

                time.sleep(POST_LOAD_SLEEP)

                image = chart.screenshot_as_png

                if image:
                    db.save(symbol, real_close, real_change, image)
                    print(f"✅ Saved: {symbol}")
                else:
                    print(f"⚠️ Empty screenshot: {symbol}")

            except Exception as e:
                print(f"❌ Error for {symbol}: {e}")

    finally:
        driver.quit()
        db.close()


if __name__ == "__main__":
    main()
