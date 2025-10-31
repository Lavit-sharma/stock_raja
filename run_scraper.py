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
import json, os, time

print("="*70)
print("TradingView Stock Scraper (Logged-In, Optimized)")
print("="*70)

# -------- CONFIGURATION --------
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 20
WAIT_VISIBLE_TIMEOUT = 6          # shorter explicit waits
RATE_LIMIT = 0.75                 # faster per-URL throttle
BATCH_SIZE_APPEND = 100           # larger batch to cut API calls
MAX_RETRIES_SCRAPE = 2
MAX_RETRIES_APPEND = 4

# Matrix slice from env
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
    try:
        sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    except gspread.WorksheetNotFound:
        sh = gc.open('Tradingview Data Reel Experimental May')
        sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)
except Exception as e:
    print(f"✗ Sheets access error: {e}"); exit(1)

company_list = sheet_main.col_values(5)  # URLs (1-indexed semantics later)
name_list = sheet_main.col_values(1)     # Names
n = len(company_list)
current_date = date.today().strftime("%m/%d/%Y")
print(f"✓ Loaded {n} companies")

# Compute unique slice for this matrix batch
start = max(1, START_INDEX)
end = min(n, start + BATCH_SIZE - 1)
print(f"Processing slice: {start}..{end}")

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

service = Service(ChromeDriverManager().install())  # auto-match Chrome version
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
driver.set_window_size(1600, 1000)
wait = WebDriverWait(driver, WAIT_VISIBLE_TIMEOUT)
print("✓ Browser initialized")

# -------- SESSION LOGIN VIA COOKIES --------
print("\n[3/5] Loading TradingView session via cookies...")
cookies_json = os.environ.get("COOKIES_JSON", "")
if not cookies_json:
    print("✗ COOKIES_JSON missing"); driver.quit(); exit(1)

try:
    driver.get("https://www.tradingview.com")
    time.sleep(1.5)
    cookies = json.loads(cookies_json)
    for ck in cookies:
        c = dict(ck)
        c.pop('sameSite', None)   # keep Selenium cookie schema clean
        c.pop('expiry', None)
        try:
            driver.add_cookie(c)
        except Exception:
            pass
    driver.refresh()
    time.sleep(2.0)
    if ("Sign in" in driver.page_source) or ("Log in" in driver.page_source):
        print("⚠ Session looks expired; aborting."); driver.quit(); exit(1)
    print("✓ Session loaded")
except Exception as e:
    print(f"✗ Cookie load error: {e}"); driver.quit(); exit(1)

# -------- SCRAPE FUNCTION (optimized waits) --------
def scrape_tradingview_values(url, retry=0):
    try:
        driver.get(url)
        # minimal condition on navigation
        try:
            wait.until(lambda d: "tradingview" in d.current_url.lower())
        except Exception:
            pass

        time.sleep(0.8)  # small render allowance
        soup = BeautifulSoup(driver.page_source, "html.parser")

        values = [n.get_text(strip=True) for n in soup.find_all("div", class_="valueValue-l31H9iuA")]
        if not values:
            nodes = soup.find_all("div", attrs={"data-name": True})
            values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]
        if not values:
            sections = soup.find_all(["span","div"], class_=lambda x: x and ("value" in str(x).lower() or "rating" in str(x).lower()))
            values = [el.get_text(strip=True) for el in sections if el.get_text(strip=True)]

        cleaned = []
        for v in values:
            v = v.replace('−','-').replace('∅','None').strip()
            if v and v not in cleaned:
                cleaned.append(v)
        return cleaned
    except Exception as e:
        if retry < MAX_RETRIES_SCRAPE:
            time.sleep(1.5 * (retry + 1))
            return scrape_tradingview_values(url, retry + 1)
        else:
            print(f"✗ Failed scrape {url}: {e}")
            return []

# -------- BATCHED APPEND WITH BACKOFF --------
buffer = []

def flush_buffer():
    if not buffer:
        return
    backoff = 2
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
                print(f"✗ Append failed after {attempt} attempts: {e}")
                return
            print(f"Append retry {attempt} in {backoff}s due to: {e}")
            time.sleep(backoff)
            backoff = min(30, backoff * 2)

# -------- MAIN LOOP --------
print("\n[4/5] Starting scraping...")
success, fail = 0, 0
for i in range(start, end + 1):
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]
    print(f"[{i}] {name} -> scraping...", end=" ")

    vals = scrape_tradingview_values(url)
    if vals:
        buffer.append([name, current_date] + vals)
        success += 1
        print(f"✓ {len(vals)} values")
        if len(buffer) >= BATCH_SIZE_APPEND:
            print(" [APPEND]")
            flush_buffer()
    else:
        print("✗ No values")
        fail += 1

    time.sleep(RATE_LIMIT)  # light throttle

print("\nFlushing remaining rows...")
flush_buffer()
driver.quit()

print("\n" + "="*70)
print(f"SCRAPING COMPLETED | Slice {start}-{end}")
print(f"Success: {success} | Failed: {fail}")
print("="*70)


