# run_scraper.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import gspread
from datetime import date
import json
import os
import time

print("="*70)
print("TradingView Stock Scraper - Logged In & Immediate Append")
print("="*70)

# -------- CONFIGURATION --------
HEADLESS_MODE = True
WAIT_TIME = 8          # Page load wait
RATE_LIMIT = 3         # Seconds between requests
MAX_RETRIES = 2

# -------- GOOGLE SHEETS SETUP --------
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
def scrape_values(url, retry_count=0):
    try:
        driver.get(url)
        time.sleep(WAIT_TIME)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = []

        # Primary selector
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

        # Clean duplicates
        cleaned = []
        for v in values:
            v = v.replace('−','-').replace('∅','None').strip()
            if v and v not in cleaned:
                cleaned.append(v)
        return cleaned

    except Exception as e:
        if retry_count < MAX_RETRIES:
            time.sleep(3)
            return scrape_values(url, retry_count+1)
        else:
            print(f"✗ Failed to scrape {url}: {str(e)}")
            return []

# -------- MAIN LOOP --------
start_index = 0
end_index = min(len(company_list), 10)  # 10 jobs for ~15-17 min
success_count = 0
fail_count = 0

for i in range(start_index, end_index):
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]
    print(f"[{i}] {name} -> scraping...", end=" ")

    vals = scrape_values(url)
    if vals:
        row = [name, current_date] + vals
        try:
            sheet_data.append_row(row, value_input_option='USER_ENTERED')
            print(f"✓ {len(vals)} values written")
            success_count += 1
        except Exception as e:
            print(f"✗ Sheet write failed: {str(e)}")
            fail_count += 1
    else:
        print("✗ No values")
        fail_count += 1
    time.sleep(RATE_LIMIT)

driver.quit()

# -------- FINAL REPORT --------
print("\n" + "="*70)
print(f"SCRAPING COMPLETED")
print(f"Success: {success_count}")
print(f"Failed : {fail_count}")
print("="*70)
