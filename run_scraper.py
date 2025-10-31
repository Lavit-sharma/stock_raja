from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
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
# Increased Timeouts for GH Actions Reliability
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 30 # Increased from 20
WAIT_VISIBLE_TIMEOUT = 10 # Increased from 6
RATE_LIMIT = 1.0 # Increased throttle slightly for reliability
BATCH_SIZE_APPEND = 100
MAX_RETRIES_SCRAPE = 3 # Increased from 2
MAX_RETRIES_APPEND = 4

# Matrix slice from env
START_INDEX = int(os.getenv("START_INDEX", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))

# -------- GOOGLE SHEETS SETUP (No changes needed, setup is correct) --------
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

# -------- CHROME BROWSER SETUP (No changes needed, setup is correct) --------
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
chrome_options.add_argument('window-size=1600x1000') # Ensure window size is added to options too

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
# driver.set_window_size(1600, 1000) # Removed as it's in options now
wait = WebDriverWait(driver, WAIT_VISIBLE_TIMEOUT)
print("✓ Browser initialized")

# -------- SESSION LOGIN VIA COOKIES (No changes needed, logic is correct) --------
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
        c.pop('sameSite', None)
        c.pop('expiry', None)
        try:
            driver.add_cookie(c)
        except Exception:
            pass
    driver.refresh()
    time.sleep(3.0) # Increased refresh wait time
    # Check for login status
    if ("Sign in" in driver.page_source) or ("Log in" in driver.page_source) or ("Join free" in driver.page_source):
        print("⚠ Session looks expired; aborting."); driver.quit(); exit(1)
    print("✓ Session loaded")
except Exception as e:
    print(f"✗ Cookie load error: {e}"); driver.quit(); exit(1)


# -------- UPDATED SCRAPE FUNCTION --------
# The core logic for extracting values is here.
def scrape_tradingview_values(url, retry=0):
    try:
        driver.get(url)
        
        # 1. Wait for a *known stable element* to ensure the page is loaded
        # TradingView's summary widget has stable data-attributes or common text
        # We wait for the 'Technicals' tab or the main price container to appear
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'container-y5UvLhKz') or contains(@class, 'container-sC-X7w4U')]")))
        except TimeoutException:
             # Fallback wait for the rating block (if it is the 'Technical Analysis' page)
            wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'wrap-KjO5sO5v')]")))
        except Exception:
             # Basic wait failed, return empty to retry or fail
             pass


        time.sleep(1.5) # Increased render allowance
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # 2. Main Value Extraction Logic (Multiple attempts for different data types)
        
        # Primary Selector for widget data (More general than previous class name)
        # This targets data inside the technicals or financials widgets
        primary_values = [n.get_text(strip=True) for n in soup.find_all("div", class_=lambda x: x and "valueValue-" in x)]
        
        # Secondary/Fallback for main price/ratings (using data attributes or generic class parts)
        fallback_values = []
        
        # a) Main Rating/Indicator (e.g., 'Strong Buy', 'Buy', etc.)
        rating_node = soup.find("span", class_=lambda x: x and "label-JWoJqJ7C" in x) # Main Price/Rating label
        if rating_node:
             fallback_values.append(rating_node.get_text(strip=True))

        # b) Collect all visible values from generic containers
        generic_values = soup.find_all(
             ["span", "div"], 
             class_=lambda x: x and ("value" in str(x).lower() or "rating" in str(x).lower() or "price" in str(x).lower())
        )
        for el in generic_values:
             text = el.get_text(strip=True)
             if text and len(text) > 1 and text not in fallback_values: # basic cleaning/filtering
                fallback_values.append(text)
        
        values = primary_values + fallback_values
        
        cleaned = []
        for v in values:
            v = v.replace('−', '-').replace('∅', 'None').strip()
            # Filter out short/irrelevant strings which can be noise
            if v and len(v) > 1 and v not in cleaned: 
                cleaned.append(v)
        
        return cleaned
        
    except (TimeoutException, WebDriverException) as e:
        if retry < MAX_RETRIES_SCRAPE:
            print(f" (Retry {retry + 1})", end="")
            time.sleep(2.0 * (retry + 1)) # Increased backoff
            return scrape_tradingview_values(url, retry + 1)
        else:
            print(f"✗ Failed scrape {url}: {e.__class__.__name__}")
            return []
    except Exception as e:
        print(f"✗ Failed scrape {url}: {e.__class__.__name__}")
        return []

# -------- BATCHED APPEND WITH BACKOFF (No changes needed, logic is correct) --------
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
            print(f" [APPEND Successful: {len(buffer)} rows]")
            buffer.clear()
            return
        except Exception as e:
            if attempt == MAX_RETRIES_APPEND:
                print(f"✗ Append failed after {attempt} attempts: {e}")
                return
            print(f"Append retry {attempt} in {backoff}s due to: {e}")
            time.sleep(backoff)
            backoff = min(30, backoff * 2)

# -------- MAIN LOOP (No changes needed, logic is correct) --------
print("\n[4/5] Starting scraping...")
success, fail = 0, 0
for i in range(start, end + 1):
    if i >= len(company_list):
        break
    
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]
    print(f"[{i}] {name} -> scraping...", end=" ")

    vals = scrape_tradingview_values(url)
    if vals:
        buffer.append([name, current_date] + vals)
        success += 1
        print(f"✓ {len(vals)} values")
        if len(buffer) >= BATCH_SIZE_APPEND:
            print(" [FLUSHING BATCH...]")
            flush_buffer()
    else:
        print("✗ No values")
        fail += 1

    time.sleep(RATE_LIMIT)

print("\nFlushing remaining rows...")
flush_buffer()
driver.quit()

print("\n" + "="*70)
print(f"SCRAPING COMPLETED | Slice {start}-{end}")
print(f"Success: {success} | Failed: {fail}")
print("="*70)
