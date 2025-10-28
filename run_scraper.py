# run_scraper.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from google.api_core.exceptions import ServiceUnavailable
from datetime import date
import json
import os
import time

print("="*70)
print("TradingView Stock Scraper (Logged-In, Formatted for Sheets)")
print("="*70)

# -------- CONFIGURATION --------
HEADLESS_MODE = True       # Change to False to see browser
WAIT_TIME = 10             # Seconds to wait for page load
RATE_LIMIT = 2             # Seconds between requests
BATCH_SIZE_APPEND = 50     # How many rows to append in batch
MAX_RETRIES = 2            # Retry failed scrapes

# -------- GOOGLE SHEETS SETUP --------
print("\n[1/5] Connecting to Google Sheets...")

creds_json = os.environ.get('GOOGLE_CREDENTIALS', '{}')
if not creds_json:
    print("✗ GOOGLE_CREDENTIALS not found in environment variables.")
    exit(1)

creds = json.loads(creds_json)
gc = gspread.service_account_from_dict(creds)

try:
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    try:
        sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    except gspread.WorksheetNotFound:
        sh = gc.open('Tradingview Data Reel Experimental May')
        sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)
except Exception as e:
    print(f"✗ Error accessing Google Sheets: {str(e)}")
    exit(1)

company_list = sheet_main.col_values(5)  # URLs
name_list = sheet_main.col_values(1)     # Names
current_date = date.today().strftime("%m/%d/%Y")
print(f"✓ Loaded {len(company_list)} companies")

# -------- CHROME BROWSER SETUP --------
print("\n[2/5] Starting Chrome browser...")
chrome_options = Options()
if HEADLESS_MODE:
    chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=chrome_options)
driver.set_window_size(1920,1080)
print("✓ Browser initialized")

# -------- SESSION LOGIN --------
print("\n[3/5] Loading TradingView session via cookies...")

cookies_json = os.environ.get("COOKIES_JSON", "")
if not cookies_json:
    print("✗ COOKIES_JSON missing. Please provide your logged-in cookies.")
    driver.quit()
    exit(1)

try:
    driver.get("https://www.tradingview.com")
    time.sleep(2)
    cookies = json.loads(cookies_json)
    for ck in cookies:
        ck = dict(ck)
        ck.pop('sameSite', None)
        ck.pop('expiry', None)
        try:
            driver.add_cookie(ck)
        except Exception:
            pass
    driver.refresh()
    time.sleep(5)
    if "Sign in" in driver.page_source or "Log in" in driver.page_source:
        print("⚠ WARNING: Session may be expired. Check cookies.")
        driver.quit()
        exit(1)
    print("✓ Session loaded successfully")
except Exception as e:
    print(f"✗ Cookie load error: {str(e)}")
    driver.quit()
    exit(1)

# -------- SCRAPE FUNCTION --------
def scrape_tradingview_values(url, retry_count=0):
    try:
        driver.get(url)
        time.sleep(WAIT_TIME)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = []

        # Primary: valueValue-l31H9iuA
        nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
        if nodes:
            values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]

        # Fallback: data-name attribute
        if not values:
            nodes = soup.find_all("div", attrs={"data-name": True})
            if nodes:
                values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]

        # Fallback: value/rating classes
        if not values:
            sections = soup.find_all(["span","div"], class_=lambda x: x and ("value" in str(x).lower() or "rating" in str(x).lower()))
            values = [el.get_text(strip=True) for el in sections if el.get_text(strip=True)]

        # Clean and deduplicate
        cleaned = []
        for v in values:
            v = v.replace('−','-').replace('∅','None').strip()
            if v and v not in cleaned:
                cleaned.append(v)
        return cleaned

    except Exception as e:
        if retry_count < MAX_RETRIES:
            time.sleep(3)
            return scrape_tradingview_values(url, retry_count+1)
        else:
            print(f"✗ Failed to scrape {url}: {str(e)}")
            return []

# -------- BATCHED APPEND --------
buffer = []

def flush_buffer():
    if not buffer:
        return
    for attempt in range(3):
        try:
            sheet_data.spreadsheet.values_append(
                'Sheet5!A1',
                {'valueInputOption':'USER_ENTERED', 'insertDataOption':'INSERT_ROWS'},
                {'values': buffer}
            )
            buffer.clear()
            return
        except Exception as e:
            wait = (attempt+1)*5
            print(f"Retrying append in {wait}s due to error: {str(e)}")
            time.sleep(wait)
    print("✗ Failed to append batch")

# -------- MAIN LOOP --------
print("\n[4/5] Starting scraping...")
start_index = 1
end_index = min(len(company_list), 1100)  # Max 1100 companies

success_count = 0
fail_count = 0

for i in range(start_index, end_index):
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]
    print(f"[{i}] {name} -> scraping...", end=" ")
    
    vals = scrape_tradingview_values(url)
    if vals:
        buffer.append([name, current_date] + vals)
        print(f"✓ {len(vals)} values")
        success_count += 1
        if len(buffer) >= BATCH_SIZE_APPEND:
            print(" [PUSH]", end="")
            flush_buffer()
    else:
        print("✗ No values")
        fail_count += 1
    time.sleep(RATE_LIMIT)

# Flush remaining
print("\nFlushing remaining rows...")
flush_buffer()
driver.quit()

# -------- FINAL REPORT --------
print("\n" + "="*70)
print(f"SCRAPING COMPLETED")
print(f"Success: {success_count}")
print(f"Failed : {fail_count}")
print("="*70)
