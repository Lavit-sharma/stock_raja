import os
import time
import json
import gspread
import pandas as pd
import mysql.connector
from datetime import datetime
from ftplib import FTP

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
STOCK_LIST_GID = 1400370843
MV2_SQL_URL = "https://docs.google.com/spreadsheets/d/1G5Bl7GssgJdk-TBDr1eWn4skcBi1OFtaK8h1905oZOc/edit"

TARGET_TABLE = "filter"

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")

CHART_WAIT_SEC = 30
POST_LOAD_SLEEP = 6
DB_RETRY = 3
MAX_DAY_TO_KEEP = 4

# ---------------- HELPERS ---------------- #
def log(msg):
    print(msg, flush=True)

def fix_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols[cols == dup].index.values.tolist()] = [
            f"{dup}_{i}" if i != 0 else dup for i in range(sum(cols == dup))
        ]
    df.columns = cols
    return df

# ---------------- DB ---------------- #
class DB:
    def __init__(self, config):
        self.config = config
        self.conn = mysql.connector.connect(**config)
        self.conn.autocommit = True

    def ensure(self):
        if not self.conn.is_connected():
            self.conn.reconnect()
        return self.conn

    def close(self):
        self.conn.close()

# ---------------- FTP ---------------- #
def upload_via_ftp(local_path, filename):
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)

    ftp.cwd("public_html/wp-content/uploads")

    try:
        ftp.mkd("screenshots")
        print("📁 Created screenshots folder")
    except:
        pass

    ftp.cwd("screenshots")

    with open(local_path, "rb") as f:
        ftp.storbinary(f"STOR {filename}", f)

    ftp.quit()

# ---------------- MAIN ---------------- #
def main():
    db = DB(DB_CONFIG)
    driver = None

    try:
        # Load Sheets
        creds = os.getenv("GSPREAD_CREDENTIALS")
        client = gspread.service_account_from_dict(json.loads(creds))

        mv2_sheet = client.open_by_url(MV2_SQL_URL).sheet1.get_all_values()
        df_mv2 = pd.DataFrame(mv2_sheet[1:], columns=[c.strip() for c in mv2_sheet[0]])
        df_mv2 = fix_duplicate_columns(df_mv2)

        # ---------------- FIXED DATE LOGIC ---------------- #
        today_str = datetime.now().strftime('%Y-%m-%d')

        delivery_max_mask = pd.Series([False] * len(df_mv2))

        for date_col in ["DATE1", "DATE2", "DATE3"]:
            if date_col in df_mv2.columns:
                parsed = pd.to_datetime(df_mv2[date_col], errors='coerce')
                delivery_max_mask |= (parsed.dt.strftime('%Y-%m-%d') == today_str)

        print("TOTAL ROWS:", len(df_mv2))
        print("MATCHED ROWS:", delivery_max_mask.sum())

        triggers = {
            "Delivery_Max": df_mv2[delivery_max_mask]
        }

        # Browser
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=Options())

        stock_ws = client.open_by_url(STOCK_LIST_URL).get_worksheet_by_id(STOCK_LIST_GID).get_all_values()
        url_map = {row[0].strip(): {'week': row[2].strip(), 'day': row[3].strip()} for row in stock_ws[1:] if row[0]}

        # Loop
        for filter_name, matched_df in triggers.items():
            print(f"🚀 Processing {filter_name}: {len(matched_df)} stocks")

            if matched_df.empty:
                print("⚠️ No matching stocks found")
                continue

            for _, row in matched_df.iterrows():
                symbol = str(row.iloc[0]).strip()
                urls = url_map.get(symbol)

                if not urls:
                    print("❌ No URL for", symbol)
                    continue

                for tf in ['day', 'week']:
                    url = urls[tf]

                    try:
                        print(f"📊 Processing {symbol} {tf}")

                        driver.get(url)
                        chart = WebDriverWait(driver, CHART_WAIT_SEC).until(
                            EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'chart-container')]"))
                        )

                        time.sleep(POST_LOAD_SLEEP)

                        filename = f"{symbol}_{tf}_{int(time.time())}.png"
                        local_path = f"/tmp/{filename}"

                        with open(local_path, "wb") as f:
                            f.write(chart.screenshot_as_png)

                        upload_via_ftp(local_path, filename)

                        public_path = f"/wp-content/uploads/screenshots/{filename}"

                        conn = db.ensure()
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO filter (symbol, timeframe, filter_type, day, screenshot_path)
                            VALUES (%s, %s, %s, 0, %s)
                        """, (symbol, tf, filter_name, public_path))
                        cur.close()

                        os.remove(local_path)

                        print(f"✅ Uploaded {symbol} ({tf})")

                    except Exception as e:
                        print(f"❌ Error {symbol} {tf}:", e)

        print("🏁 Finished")

    except Exception as e:
        print("❌ Fatal:", e)
    finally:
        if driver:
            driver.quit()
        db.close()

if __name__ == "__main__":
    main()
