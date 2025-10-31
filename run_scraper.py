from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
from google.api_core.exceptions import ServiceUnavailable
from datetime import date
import json, os, time, random

print("="*70)
print("TradingView Stock Scraper (Stable Enhanced)")
print("="*70)

# -------- CONFIG --------
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 25
WAIT_VISIBLE_TIMEOUT = 10
RATE_LIMIT = 1.2
BATCH_SIZE_APPEND = 100
MAX_RETRIES_SCRAPE = 3
MAX_RETRIES_APPEND = 4
SCROLL_STEPS = 3

START_INDEX = int(os.getenv("START_INDEX", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))

# -------- GOOGLE SHEETS SETUP --------
print("\n[1/5] Connecting to Google Sheets...")
creds_json = os.environ.get('GOOGLE_CREDENTIALS', '')
if not creds_json:
    print("✗ GOOGLE_CREDENTIALS missing"); exit(1)

gc = gspread.service_account_from_dict(json.loads(creds_json))
try:
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
except Exception as e:
    print(f"✗ Sheets access error: {e}"); exit(1)

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
n = len(company_list)
current_date = date.today().strftime("%m/%d/%Y")
print(f"✓ Loaded {n} companies")

start = max(1, START_INDEX)
end = min(n, start + BATCH_SIZE - 1)
print(f"Processing slice: {start}..{end}")

# -------- BROWSER SETUP --------
print("\n[2/5] Starting Chrome...")
opts = Options()
if HEADLESS_MODE: opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")
opts.add_argument("--disable-gpu")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=opts)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
wait = WebDriverWait(driver, WAIT_VISIBLE_TIMEOUT)
print("✓ Chrome ready")

# -------- COOKIES LOGIN --------
print("\n[3/5] Loading cookies...")
cookies_json = os.environ.get("COOKIES_JSON", "")
if not cookies_json:
    print("✗ COOKIES_JSON missing"); driver.quit(); exit(1)

driver.get("https://www.tradingview.com")
for c in json.loads(cookies_json):
    c.pop('sameSite', None)
    c.pop('expiry', None)
    try:
        driver.add_cookie(c)
    except: pass
driver.refresh()
time.sleep(2)
if "Sign in" in driver.page_source or "Log in" in driver.page_source:
    print("⚠ Session invalid"); driver.quit(); exit(1)
print("✓ Session active")

# -------- SCRAPE FUNCTION --------
def scrape_tradingview_values(url, attempt=1):
    try:
        driver.get(url)
        wait.until(lambda d: "tradingview" in d.current_url.lower())
        time.sleep(1.5 + random.uniform(0.5, 1.5))

        # scroll a bit to load lazy sections
        for _ in range(SCROLL_STEPS):
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/3);")
            time.sleep(0.8)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [v.get_text(strip=True) for v in soup.select("div.valueValue-l31H9iuA")]
        if not values:
            values = [v.get_text(strip=True) for v in soup.find_all("div", attrs={"data-name": True})]
        cleaned = list(dict.fromkeys(v.replace('−','-').strip() for v in values if v.strip()))

        # validate completeness: expect at least ~10 data points
        if len(cleaned) < 10 and attempt < MAX_RETRIES_SCRAPE:
            print(f" (retry {attempt})", end="")
            time.sleep(2 * attempt)
            return scrape_tradingview_values(url, attempt + 1)
        return cleaned
    except Exception as e:
        if attempt < MAX_RETRIES_SCRAPE:
            time.sleep(2 * attempt)
            return scrape_tradingview_values(url, attempt + 1)
        print(f"✗ Scrape fail: {url} -> {e}")
        return []

# -------- SHEETS APPEND --------
buffer = []

def flush_buffer():
    if not buffer: return
    for attempt in range(1, MAX_RETRIES_APPEND + 1):
        try:
            sheet_data.spreadsheet.values_append(
                'Sheet5!A1',
                {'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'},
                {'values': buffer}
            )
            buffer.clear()
            return
        except Exception as e:
            if attempt == MAX_RETRIES_APPEND:
                print(f"✗ Append failed: {e}")
                return
            time.sleep(3 * attempt)

# -------- MAIN LOOP --------
print("\n[4/5] Scraping starts...")
success, fail = 0, 0
for i in range(start, end + 1):
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]
    print(f"[{i}] {name} ->", end=" ")
    vals = scrape_tradingview_values(url)
    if vals:
        buffer.append([name, current_date] + vals)
        success += 1
        print(f"✓ {len(vals)} vals")
        if len(buffer) >= BATCH_SIZE_APPEND:
            flush_buffer()
    else:
        print("✗ none")
        fail += 1
    time.sleep(RATE_LIMIT)

flush_buffer()
driver.quit()
print(f"\n✓ Completed | Success: {success} | Fail: {fail}")
