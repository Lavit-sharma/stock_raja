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

# ---------------- DB CLASS ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.connect()

    def connect(self):
        for attempt in range(5):
            try:
                print(f"🔌 Connecting to DB (attempt {attempt+1})...")
                
                self.conn = mysql.connector.connect(
                    host=self.config["host"],
                    user=self.config["user"],
                    password=self.config["password"],
                    database=self.config["database"],
                    connection_timeout=10
                )

                print("✅ DB Connected Successfully")
                return

            except Exception as e:
                print(f"❌ DB connection failed: {e}")
                time.sleep(3)

        raise Exception("🚨 Could not connect to DB after retries")

    def fetch_symbols(self):
        try:
            print("📊 Fetching stocks with change >= 7...")

            query = f"""
                SELECT Symbol, real_close, real_change
                FROM {SOURCE_TABLE}
                WHERE CAST(real_change AS DECIMAL(10,2)) >= 7
            """

            cur = self.conn.cursor(dictionary=True)
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()

            print(f"✅ Found {len(rows)} stocks")
            return rows

        except Exception as e:
            print(f"❌ Fetch error: {e}")
            return []

    def save(self, symbol, real_close, real_change, image):
        try:
            query = f"""
                INSERT INTO {TARGET_TABLE} 
                (symbol, real_close, real_change, screenshot)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                real_close = VALUES(real_close),
                real_change = VALUES(real_change),
                screenshot = VALUES(screenshot)
            """

            cur = self.conn.cursor()
            cur.execute(query, (symbol, real_close, real_change, image))
            cur.close()

            print(f"💾 Saved: {symbol}")

        except Exception as e:
            print(f"❌ Save error for {symbol}: {e}")

    def close(self):
        if self.conn:
            self.conn.close()
            print("🔌 DB Closed")


# ---------------- SELENIUM ---------------- #
def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )


# ---------------- MAIN ---------------- #
def main():
    print("🚀 Script started...")

    # Debug env
    print("DB_HOST:", os.getenv("DB_HOST"))
    print("DB_USER:", os.getenv("DB_USER"))

    db = DB(DB_CONFIG)
    driver = get_driver()

    try:
        rows = db.fetch_symbols()

        if not rows:
            print("⚠️ No stocks found. Exiting.")
            return

        for row in rows:
            symbol = str(row["Symbol"]).strip()
            real_close = row["real_close"]
            real_change = float(row["real_change"])

            print(f"📈 Processing: {symbol} ({real_change}%)")

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
                else:
                    print(f"⚠️ Empty screenshot: {symbol}")

            except Exception as e:
                print(f"❌ Error for {symbol}: {e}")

    finally:
        driver.quit()
        db.close()
        print("🏁 Script finished")


if __name__ == "__main__":
    main()
